# charlie-server

Charlie is a self-hosted note-taking and file sharing application. With charlie your notes which will be stored on your device, backed up to
your server instance and distributed to your other devices. Charlie also lets
you share files from one device to another, however files are not backed up at
the server and instead streamed from one device to the other with the server
as an intermediary. 

⚠️ This project is still under development and missing a mobile app.

This repository contains the server side of charlie. There is currently only a desktop
client available, see charlie-fx.

## Features
- Sync notes and files between devices
- Implements password authentication with auth tokens and refresh tokens functionality

## How it works
The server will store your notes in an SQLite database, distribute them to 
connected devices and handle file requests. Currently, all devices including
the server act as replicas for your notes. When one device requests a file, the server
notifies the target device which stores the file. This device then does a corresponding
request on the server and the contents of the file are streamed from second to the first.

The server was implemented to run on machines like for example a Raspberry Pi,
but you can run it on your own laptop / computer as well. Keep in mind that 
running the server and charlie-fx on the same device will store your notes 
twice there. An updated version of charlie might improve on this.

## Technology
This project only depends on:

```bash
github.com/alecthomas/kong
github.com/google/uuid
github.com/gorilla/websocket
github.com/mattn/go-sqlite3
golang.org/x/crypto
```

The server exposes an HTTP/Websockets API. See server.go for that.

## Build
This project was built using go1.22.3.

```bash
$ go build -o build/<arch>/charlie-server)
```

## Usage

You first have to create a credentials file in which you write a strong master-password.
Then you use the `create-instance` command on the server binary using the credentials file to
create a config file for the server. This config file will specify on which address the server should 
listen for requests. The generated config file will make the server listen on `localhost:8080`, because you
should use the server behind a reverse proxy with tls configured. However the config file is a plain
text file and you can change it at will. The instance name is just the name of the folder
in which the server will put all server related files.

`create-instance` command:
```bash
$ ./charlie-server create-instance <instance-name> <credentials-file>
```

`run-server` command:
```bash
$ ./charlie-server run-server --config=<config-file-path>
```

You can optionally specify a logging level using `--logging-level`. Possible values are
one `"minimal", "default", "verbose"`

A server config file looks like:
```json
{
    "server_address": <addr>,
    "work_dir": <path>,
    "authentication_hash": <bcrypt-hash>
}
```