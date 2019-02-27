## RPC Commands

### DB.create 

Creates database. 

```
DB.create ["name"]
DB.create ["name", { ... config ... }]
```

- **name** - name of the database. Can't be empty. Can't contain zero character.
- **config** - optional - object contains various field. If not specified, default config is used

```
{
	"storage":"permanent|memory"
}
```

- **storage** - default value **permanent** - Permament storage is created on disk and persists even server crashes or it is restarted. Memory storage is created in memory and it is wiped out on server crash or restart. Memory storage is much faster than permanent storage. The memory storage also can be bootstraped on startup using replication from permanent storage

**Note to memory storage** - only database name and its configuration and security settings is saved between restarts. Other objects such a document storage, views, reduce, design documents, local documents, etc is held in memory and lost on server's restart

### DB.delete

Deletes the database

```
DB.delete ["name"]
```

Removes everything related to database specified by its name. Depend on size of database operation can take a some time. However it is possible, that command is completted sooner than final deletion is done. 

### DB.list

Lists all databases including its configuration

```
DB.list []
```

Example of result

```
{
"config":	{
	"history_max_age":	86400000,
	"history_max_count":	3,
	"history_max_deleted":	0,
	"history_min_count":	2,
	"logsize":	100
},
"id":	0,
"name":	"one",
"storage":	"permanent"
}
```

- **config** - contains configuration (see DB.setConfig)
- **id** - internal ID of the database
- **name** - name of the database
- **storage** -type of storage

 
### DB.rename

Renames database 

```
DB.rename["oldname","newname"]
```

Returns: true

### DB.setConfig

Sets configuration of the database

```
DB.setConfig["database_name",{...config...}]
```

- **logsize** - [number] specifies maximum count of item in "log" field of the document. Each item occupies 9 bytes, so default value 100 means, that every revision has 900 bytes reserved for log

- **history_max_age** - [number] specifies how old revisions are deleted in milliseconds. 
The housekeeping of this revisions are performed after update of the document, so it does
mean that there can be much older revisions until the document is updated. Only after
the update older revisions are collected. If this value is zero, no history is stored (unless **history_min_count** is set)

- **history_max_count** - [number] specifies max count of old revisions. The database will
erase revisions above the numer even if they are not old enough by number **history_max_age**. However it will not erase oldest revisions. Instead it tries to keep same distance of time between each revision by erasing revisions between. This number can't be less than 2

- **history_max_deleted** - [number] specifies maximum count of revisions for deleted document. Default value 0 specifies, that whole history is wiped out when document is deleted (just its tombstone is kept)

- **history_min_count** - [number] specifies minimum count of revisions kept even if they
are older then **history_max_age**



### DB.changes

Receives recent changes of the database

```
DB.changes ["databae_name", {...config...}]
```

The configuration object
 
```
{
	"log":   (boolean, optional),
	"data":   (boolean, optional),
	"deleted":   (boolean, optional),
	"since": (number, optional)
	"descending": (boolean, optional),
	"limit": (number, optional)
	"offset": (number, optional)
	"timeout": (number, optional)
	"notify": (string, optional)
	"filter": (filter specification, optional)
```

* **log** - optional, if set true, each returned document will have **log** field
* **data** - optional, if set true, each returned document will have **data** field
* **deleted** - optional, if set false, function will skip deleted documents
* **since** - specifies sequence number of last seen change.  
* **descending** - optional, if set true, the list will be reversed
* **limit** - limit of returned results
* **offset** - count of records to skip
* **timeout** - if timeout set, the function will wait for max amout timeout (in milliseconds) for the first change. 
* **notify** - specifies name of JSONRPC notification which will be used to deliver changes to the client. In this case **timeout** specifies total time of running function,
it doesn't stop on the first record. The string must be unique on the server, otherwise,
the function can return 409 conflict.
* **