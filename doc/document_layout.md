## Document layout

Layout is fixed, some fields are optional

```
{
   "id": "<document_id>",
   "rev": "<document_revision>",
   "data": <user data>,
   "log": ["<log revisions>"],
   "conflicts":["<conflicts>"],
   "deleted": true/undefined,
   *timestamp* : <number>
}
```

### Legend (retrieve)

- **id** - contains document ID. The document can be retrieved under that ID
- **rev** - contains document revision as a string. 
- **data** - contains any arbitrary JSON as an user specified data. This field can be surpressed while one need to retrieve only metadata without possible large section containing user data.
- **log** - contains list of historical revisions (array of strings) starting by the recent one. The list can have zero, one or many items depend on how much of the history is recorded. Not all revisions can be also available to retrieve. This
field normally doesn't appear unless it is requested.
- **conflicts** - appears always if the document is in conflicted state. Contains list of revisions (array of strings) of alternative revisions. These revisions must not appear in the log
- **deleted** - appear only if document is marked as deleted. This field doesn't appear unless it is requested. If the document is deleted and the field is not requested then document cannot be retrieved - considered as not existing
- **timestamp** - contains timestamp of creation of this revision. The timestamp is in milliseconds since epoch.

### Legend (insert/update)

- **id** - This field is mandatory and contains ID of document. The string must not be empty and must not contain character
code zero. Other characters are allowed (including UTF-8). Note that ID is used as key to many indexes, so try to keep
ID smallest possible to reduce space requirement.
- **rev** - Specify revision as string. The string must be BASE62 number with up to 10 digits (64 bit number). This field
has different meaning during update in compare to replication. For new document, the **rev** must not be specified. For
update of the document, the **rev** must contain ID of the revision being updated. The database generates new revision ID and sends it back to the client. For replication, the **rev** must contain revision ID of current revision. In this mode, there must be also **log** field containing other historical revisions.   
- **data** - contains user specified data, of any type. The field must be defined, but can be set to **null**.
- **log** - this field is mandatory for replication, otherwise it is ignored. In replication mode it contains list of 
revision IDs as array of strings, where the first revision ID refers to most recent revision.
- **conflicts** - Sends update of the document in conflicted state. Contains list of revisions IDs marked as conflict. These
revision should not be any historical revisions. In most common use cases the client removes this field to mark conflict resolved. Replicator can specify current top level document revision as conflict if it needs to force push its revision
as top level revision even if its history doesn't match the current top-level document history.
- **deleted** - by setting this field to true causes, that document is deleted. Depend on settings, it can cause that all or most of historical revisions are also deleted. However current revision in deleted state is maintained as tombstone to achieve correct replication of that deleted state
- **timestamp** - this field is ignored unless replication mode is running. In replication mode, this field is replicated to the target database along with the document. Otherwise the timestamp is always set to current time.
