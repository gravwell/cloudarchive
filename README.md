# Gravwell Cloud Archive

This repository contains code which implements the Gravwell Cloud Archive server, along with an example client and some other utilities.

* `server` contains the reference Cloud Archive server.
* `testclient` is an interactive command-line tool to interact with a Cloud Archive server.
* `usertool` is a utility for managing a password database as used by the Cloud Archive server.
* `configtool` is a small utility which attempts to generate a `gravwell.conf` for a set of archived shards.
* `pkg` contains packages used by the Cloud Archive system.

## Running a Cloud Archive server

Until the Cloud Archive server is packaged officially, you can set it up manually. Make sure you have [Go](https://golang.org) installed, to build the programs.

Before running the server, you'll need to set up the password database and the config file. For simplicity, we'll assume all cloud archive information will be kept in `/opt/cloudarchive`.

### Initializing the Password Database

Use the `usertool` command to set up the password database with an entry for your customer number:

```
cd usertool
go build
./usertool -action useradd -id <customer number> -passfile /opt/cloudarchive/cloud.passwd
```

The tool will prompt for the passphrase to use for the specified customer number.

Note: You can find your customer number on the License page of the Gravwell UI.

### Configuration

The following config file will make the server archive incoming data to `/opt/cloudarchive/storage`. It listens for clients on port 8886, using the specified TLS cert/key pair for encryption. The `Password-File` parameter points at the password database set up earlier.

```
[Global]
Listen-Address="0.0.0.0:8886"
Cert-File=/opt/cloudarchive/cert.pem
Key-File=/opt/cloudarchive/key.pem
Password-File=/opt/cloudarchive/cloud.passwd
Log-Level=INFO
Backend-Type=file
Storage-Directory=/opt/cloudarchive/storage
```

The following config archives incoming data shards to an FTP server instead of the local disk. Note the specification of the FTP-Server; the FTP-Username and FTP-Password fields should be for a valid account on that FTP server.

```
[Global]
Listen-Address="0.0.0.0:8886"
Cert-File=/opt/cloudarchive/cert.pem
Key-File=/opt/cloudarchive/key.pem
Password-File=/opt/cloudarchive/cloud.passwd
Log-Level=INFO
Backend-Type=ftp
Storage-Directory=/opt/cloudarchive/storage
FTP-Server=ftp.example.org:21
FTP-Username=cloudarchiveuser
FTP-Password=ca_secret_password
```

### Build and install the binary

Install the server binary into `/opt/cloudarchive`:

```
cd server
go build
cp server /opt/cloudarchive
```

### Create user / set ownership

For security reasons, we'll create a system user to run the Cloud Archive service and give it ownership of `/opt/cloudarchive`:

```
adduser -S -h /opt/cloudarchive -H -D -g "Cloud Archive Daemon User" -G cloudarchive cloudarchive
chown -R cloudarchive:cloudarchive /opt/cloudarchive
```

### Run the server

Now that everything is set up, we create a service file in `/etc/systemd/system/cloudarchive.service`:


```
[Unit]
Description=Gravwell Cloud Archive Service
After=network-online.target

[Service]
Type=simple
ExecStart=/opt/cloudarchive/server -config-file /opt/cloudarchive/server.conf
WorkingDirectory=/opt/cloudarchive
Restart=always
User=cloudarchive
Group=cloudarchive
StandardOutput=journal
StandardError=journal
LimitNPROC=infinity
LimitNOFILE=infinity
PIDFile=/var/run/gravwell_cloudarchive.pid
TimeoutStopSec=5
KillMode=process
KillSignal=SIGINT
```

Then we enable & run it:

```
systemctl enable cloudarchive.service
systemctl start cloudarchive.service
```

### Configure Gravwell

To make Gravwell send archived shards to the Cloud Archive server, add a `Cloud-Archive` stanza to `gravwell.conf`:

```
[Cloud-Archive]
	Archive-Server=cloudarchive.example.org
	Archive-Shared-Secret="mypassword"
```

(The `Archive-Shared-Secret` field should match the password you set for your customer number in the password database)

Shards will not be archived unless the well has the `Archive-Deleted-Shards=true` and `Delete-Frozen-Data=true` parameters set.

Refer to the [documentation for Cloud Archive](https://docs.gravwell.io/configuration/archive.html) for more information.