# sofadb
CouchDB inspired document database written in C++. In planning state

## Motivation

To bring standalone database server offering key=document storage,
materialized map/reduces views and indexes, master to master replication
and heavy server-side scripting

The main purpose is to offer programable database and server which acting as
a server to modern and popular PWA applications

## Planned technologies

 * Key-Value storage database as backend. It is designed for LevelDB, but can 
  be adapted to other Key-Value engines
 * ImtJSON library
 * SimpleServer TCP/HTTP/WebSocket library
 * SpiderMonkey - Javascript scripting engine
 
## Planned features
 
 * Multiple databases at single server
 * Document index
 * Adjustable history depth (from zero to infinite)
 * ID and Revision similar to CouchDB
 * however metadata are not inside of the document
 * Ordered write (Database retains write order)
 * Streaming changes
 * No conflicted documents - all conflicts are automatically merged when detected
   * implicit 3-way merge or scripted merge
 * Scripted views
 * Scripted reduces
 * Multiple reduces from single view
 * Scripted join views (combine multiple views into single response)
 * Postprocessing
 * read access control per document
 * scripted validation with feature to query other documents need for validation
 * modify before store script - ability to modify document before it is stored
 * master to master replication 
 
### Not planned features (in comparison to CouchDB)
 * Clusters - each server offers just single node like CouchDB 1.7+. With continuous replication, 
   data can be replicated to other nodes to achieve that all nodes have the same content
    * Clustering is planned as superior proxy, which is able to map requests to separate nodes and control 
      the replication - but later
      
      
## API

 * REST
 * JSONRPC over HTTP
 * JSONRPC over TCP
 * JSONRPC over WebSocket
 
 AF_INET or AF_UNIX support
 
 
 
 
 
