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

 