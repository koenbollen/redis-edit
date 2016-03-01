package main // import "github.com/koenbollen/redis-edit"

import (
	"bytes"
	"encoding/json"
	"fmt"
	"go/doc"
	"io/ioutil"
	"os"
	"os/exec"
	"regexp"
	"strings"

	"github.com/docopt/docopt-go"

	"gopkg.in/redis.v3"
)

const usage = `Usage:
  redis-edit [options] <key>
  redis-edit (--help | --version)

This Redis utility will get a key and open it in your favorite EDITOR, when
changes were made it will write the change back to the same Redis key.

For editing the environment variable EDITOR is used or a few defaulted editors
are tried (like nano).

To edit more complex data types redis-edit will convert the data into a JSON
representation which can be edited and is converted back when written to Redis.

Arguments:
  <key>              The redis key to edit. Currently only the following types
                     are supported: string, list, set, hash, zset
Options:
  --help             Show this screen.
  --version          Show version.
  -h <hostname>      Server hostname [default: 127.0.0.1].
  -p <port>          Server port [default: 6379].
  -s <socket>        Server socket (overrides hostname and port).
  -a <password>      Password to use when connecting to the server.
  -n <db>            Database number [default: 0].
  -r --raw           Raw writes, don't validate edits (only for string)
`

var gitref = `unknown version`

var editors = []string{"nano", "pico", "vim", "vi", "emacs"}

type config struct {
	validate bool
}
type accessor struct {
	get      func(*redis.Client, string) ([]byte, error)
	validate func([]byte) error
	write    func(*redis.Client, string, []byte) error

	description string
}

var accessors map[string]accessor

func cli(args []string) (string, *config, *redis.Options) {
	arguments, _ := docopt.Parse(usage, args, true, "redis-edit "+gitref, true)

	options := &redis.Options{}
	if arguments["-s"] != nil {
		options.Network = "unix"
		options.Addr = arguments["-s"].(string)
	} else {
		options.Addr = arguments["-h"].(string) + ":" + arguments["-p"].(string)
	}
	if password, ok := arguments["-a"].(string); ok {
		options.Password = password
	}
	if db, ok := arguments["-n"].(int64); ok {
		options.DB = db
	}

	c := &config{
		validate: arguments["--raw"].(bool),
	}

	return arguments["<key>"].(string), c, options
}

func main() {
	defer func() {
		if err := recover(); err != nil {
			fmt.Println("error:", err)
		}
	}()

	key, config, options := cli(os.Args[1:])
	client := redis.NewClient(options)

	keytype, err := client.Type(key).Result()
	if err == redis.Nil || keytype == "none" {
		keytype = "string"
	} else if err != nil {
		panic(fmt.Errorf("unable to get key: %v", err))
	}

	accessor, found := accessors[keytype]
	if !found {
		panic(fmt.Errorf("redis type %q not supported", keytype))
	}
	data, err := accessor.get(client, key)
	if err != nil {
		panic(fmt.Errorf("unable to get key: %v", err))
	}

	fp, _ := ioutil.TempFile("", "redis-edit")
	defer fp.Close()
	defer os.Remove(fp.Name())
	if accessor.description != "" {
		doc.ToText(fp, accessor.description, "# ", "", 79)
	}
	fp.Write(data)

	editor, args := editor()
	args = append(args, fp.Name())
	cmd := exec.Command(editor, args...)
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	err = cmd.Run()
	if err != nil {
		panic(fmt.Errorf("an error occurred while editing: %v", err))
	}
	newdata, err := ioutil.ReadFile(fp.Name())
	if err != nil {
		panic(fmt.Errorf("failed to read tempfile: %v", err))
	}

	if bytes.Compare(data, newdata) != 0 {

		if accessor.description != "" {
			comments := regexp.MustCompile(`(?m:^\s*#.*$\n?)`)
			newdata = comments.ReplaceAll(newdata, nil)
		}

		if !config.validate || keytype != "string" {
			err = accessor.validate(newdata)
			if err != nil {
				panic(fmt.Errorf("invalid json: %v", err))
			}
		}

		accessor.write(client, key, newdata)
	}
}

func init() {
	var err error
	var nothing interface{}
	var list []string
	var hash map[string]string
	var zset []redis.Z
	var zsetMap map[string]float64
	shouldValidate := false

	accessors = make(map[string]accessor)

	accessors["string"] = accessor{
		get: func(client *redis.Client, key string) ([]byte, error) {
			data, err := client.Get(key).Bytes()
			if err == redis.Nil {
				err = nil
			}
			jsonFault := json.Unmarshal(data, &nothing)
			_, isMap := nothing.(map[string]interface{})
			_, isArray := nothing.([]interface{})
			shouldValidate = jsonFault == nil && (isMap || isArray)
			return data, err
		},
		validate: func(data []byte) error {
			if shouldValidate {
				return json.Unmarshal(data, &nothing)
			}
			return nil
		},
		write: func(client *redis.Client, key string, data []byte) error {
			return client.Set(key, data, 0).Err()
		},
	}

	accessors["list"] = accessor{
		description: "This is a JSON representation of the data type LIST.\n" +
			"Edit, but don't change it's type!",
		get: func(client *redis.Client, key string) ([]byte, error) {
			list, err = client.LRange(key, 0, -1).Result()
			if err == redis.Nil {
				err = nil
			}
			data, err := json.MarshalIndent(list, "", "  ")
			return data, err
		},
		validate: func(data []byte) error {
			return json.Unmarshal(data, &list)
		},
		write: func(client *redis.Client, key string, data []byte) error {
			_, err := client.Pipelined(func(pipe *redis.Pipeline) error {
				pipe.Del(key)
				pipe.LPush(key, list...)
				return nil
			})
			return err
		},
	}

	accessors["set"] = accessor{
		description: "This is a JSON representation of the data type SET.\n" +
			"Edit, but don't change it's type!",
		get: func(client *redis.Client, key string) ([]byte, error) {
			list, err = client.SMembers(key).Result()
			if err == redis.Nil {
				err = nil
			}
			data, err := json.MarshalIndent(list, "", "  ")
			return data, err
		},
		validate: func(data []byte) error {
			return json.Unmarshal(data, &list)
		},
		write: func(client *redis.Client, key string, data []byte) error {
			_, err := client.Pipelined(func(pipe *redis.Pipeline) error {
				pipe.Del(key)
				pipe.SAdd(key, list...)
				return nil
			})
			return err
		},
	}

	accessors["hash"] = accessor{
		description: "This is a JSON representation of the data type HASH.\n" +
			"Edit, but don't change it's type!",
		get: func(client *redis.Client, key string) ([]byte, error) {
			hash, err = client.HGetAllMap(key).Result()
			if err == redis.Nil {
				err = nil
			}
			data, err := json.MarshalIndent(hash, "", "  ")
			return data, err
		},
		validate: func(data []byte) error {
			hash = make(map[string]string)
			return json.Unmarshal(data, &hash)
		},
		write: func(client *redis.Client, key string, data []byte) error {
			_, err := client.Pipelined(func(pipe *redis.Pipeline) error {
				pipe.Del(key)
				for field, value := range hash {
					pipe.HSet(key, field, value)
				}
				return nil
			})
			return err
		},
	}

	accessors["zset"] = accessor{
		description: "This is a JSON representation of the data type ZSET.\n" +
			"Edit, but don't change it's type!",
		get: func(client *redis.Client, key string) ([]byte, error) {
			zset, err = client.ZRangeWithScores(key, 0, -1).Result()
			if err == redis.Nil {
				err = nil
			}
			zsetMap = make(map[string]float64)
			for _, z := range zset {
				zsetMap[z.Member.(string)] = z.Score
			}
			data, err := json.MarshalIndent(zsetMap, "", "  ")
			return data, err
		},
		validate: func(data []byte) error {
			zsetMap = make(map[string]float64)
			err = json.Unmarshal(data, &zsetMap)
			if err != nil {
				return err
			}
			zset = make([]redis.Z, 0)
			for member, score := range zsetMap {
				zset = append(zset, redis.Z{Score: score, Member: member})
			}
			return nil
		},
		write: func(client *redis.Client, key string, data []byte) error {
			_, err := client.Pipelined(func(pipe *redis.Pipeline) error {
				pipe.Del(key)
				pipe.ZAdd(key, zset...)
				return nil
			})
			return err
		},
	}
}

func editor() (executable string, arguments []string) {
	editor := os.Getenv("EDITOR")
	for editor == "" && len(editors) != 0 {
		editor, editors = editors[0], editors[1:]
		_, err := exec.LookPath(editor)
		if err != nil {
			editor = ""
		}
	}
	if editor == "" {
		panic("no EDITOR environment variable found")
	}
	parts := strings.Split(editor, " ")
	return parts[0], parts[1:]
}
