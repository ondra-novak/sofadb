## RPC Commands

- [DB.create](#dbcreate)
- [DB.delete](#dbdelete)
- [DB.list](#dblist)
- [DB.rename](#dbrename)
- [DB.setConfig](#dbsetconfig)
- [DB.changes](#dbchanges)
- [DB.stopChanges](#dbstopchanges)
- [Doc.put](#docput)
- [Doc.get](#docget)
- [Doc.changes](#docchanges)


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
* **notify** - specifies name of JSONRPC notification which will be used to deliver changes to the client. In this case **timeout** is ignored. It doesn't stop on the first record. The string must be unique on the server, otherwise, the function can return 409 conflict. To stop this function, use **DB.stopChanges**
* **filter** - specifies filtes, see **filter definition**

### DB.stopChanges

Stops receiving changes 

```
DB.stopChanges["notify_name"]
```

Function terminates pending **DB.changes** command in notification mode. The argument contains name of the notification. The pending **DB.changes** returns with last processed sequence number.

Function returns **true**

### Doc.put

Puts document(s) to the database

```
Doc.put["database_name",{... document update...},...]
```

Function puts document to the database specified by **database_name** as first argument. Second and 
other arguments can contains documents. See [document layout](document_layout.md) for more informations.

**Function returns** array, where each item corresponds to an item in the request, There can be two forms of the result

#### Success

``` 
{
	"id": "document_id",
	"rev": "document_revision"
}
```
If the document has been merged (because there were solvable conflict), the function returns two revisions
``` 
{
	"id": "document_id",
	"rev": ["document_revision","merged_revision"]
}
```
The first revision is revision of created document. Any future update can refer this revision, but it will always in conflict with current head revision and merged. The second revision is new head revision. You should use head revision to update your internal state of the document and for future modifications use this revision to avoid future conflicts and merges.

#### Failure

```
{
"code":	<code>,
"error": true,
"id":	"document_id",
"message":	"description"
}
```

The failure item has always **error** field set to **true**. The **code** contains code of error and **message** contains human readable description

Most common code - **409** - conflict - means, that update of the document cannot be merged. This can be due various reason.
 - Revision ID doesn't refer correct revision
 - Revision ID refers revision which is no longer available
 - Merge conflict - The one of modified fields has been also modified in the other update
 
 
### Doc.get

Reads document(s} from database

```
Doc.get ["database_name", <doc_spec>,...]
```

Reads one or multiple documents from the database. The **doc_spec** can be either **string** or **object**.

- **string** - contains document id to read. The function returns head revision if the document exists and it is not deleted. 
- **object** - offers more options

```
{
	"id":  (string, optional),
	"rev":  (string, optional),
	"log":  (boolean, optional),
	"data":  (boolean, optional),
	"deleted":   (boolean, optional),
	"prefix":   (string, optional),
	"start_key":   (string, optional),
	"end_key:   (string, optional),
	"offset:   (number, optional),
	"limit:   (number, optional),
	"descending:  (boolean, optional)
}
```

Function can return either single document or list of document. If object contains **id** the single document will be returned. If the objct contains **prefix** or **start_key** and **end_key**, then list of documents will be returned. If the object contains none of above of keys, the
other fields just modifies output format for all items following this object

- **log** - if set to **true**, the revision log is included to each document
- **data** - if set to **true** then the data are included to each document. This is default value, so you can use **data** to set **false** if you don't need data
- **deleted** - if set to false (default), deleteded documents are not available and hidden. Request for deleted document results to error. If set to true, then deleted documents are also visible and contain **deleted:true** field
- **id** - specifies document id to retrieve
- **rev** - revision to retrieve. If the field missing then head revision is retrieved
- **prefix** - allows to retrieve list of documents starting by the prefix. Use "" to retrieve all documents
- **start_key** - allows to retrieve list of documents starting by the given id
- **end_key** - specifies end key of the range where first key is defined using **start_key**. The **end_key** is always exluded. If the **end_key** is ordered before **start_key**, then descending
list is returned
- **descending** - used along with **prefix** allows ti retrieve descending list
- **offset** - offset in list
- **limit** - limits count of items in list  


### Doc.changes

Reads document's history

```
Doc.changes ["database_name", "document_id",{... cfg ...} ]
```

Configuration is optional

```
{
	"log":  (boolean, optional),
	"data":  (boolean, optional),
	"deleted":   (boolean, optional),
	"since":   (number, optional),
	"offset:   (number, optional),
	"limit:   (number, optional),
	"descending:  (boolean, optional)
}
```

- **log** - if set to **true**, the revision log is included to each document
- **data** - if set to **true** then the data are included to each document. 
- **deleted** - if set to false, deleted documents are not available and hidden. Request for deleted document results to error. If set to true (true), then deleted documents are also visible and contain **deleted:true** field
- **since** - retrieves revisions after given timestamp (in milliseconds from epoch). If missing, all revisions are returned
- **descending** - returns list descending (starting by most recent revision)
- **offset** - offset in list
- **limit** - limits count of items in list

  
 

 