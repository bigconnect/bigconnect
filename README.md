<p align="center">
  <img src="https://github.com/bigconnect/bigconnect/raw/master/docs/logo.png" alt="BigConnect Logo"/>
  <br>
  The multi-model Big Graph Store<br>
</p>

# Welcome to BigConnect!
[bigconnect.io](https://bigconnect.io)

A massively scalable Big Data graph store designed for holding and querying vast amounts of raw data. 

It can store objects and links with multi-valued properties and raw data, fast queries, aggregations, distributed data processing pipelines and super-strong security (row and property-level).

Can be used to build solutions that cover most data analytics use cases and industry requirements, from financial analysis to social media and cybersecurity. 

Can be deployed on-premise, in the cloud, or both.

## Features:

* Dynamic data model (concepts, relations, properties, tables), multivalued properties and property metadata support
* Authorizations and visibilities for rows and columns
* Indexing for fast search: full-text, fuzzy, spatial, aggregations
* Async distributed processing pipeline for applying custom logic on import/update/delete operationg
* Cypher query language support
* Super extensible and modular
* Beautiful feature-rich visual explorer
* Developed in Java

### Supported backends
- Memory
- RocksDB
- Apache Accumulo
- ElasticSearch

## Installation
BigConnect can be run just about anywhere using the deployment methods described below.

### Docker
To run the latest build via Docker with the RocksDB backend and embeded ElasticSearch, just type
```
docker run -d -p 10242:10242 -p 10243:10243 bigconnect/bigconnect
```
If you would like to persist data between runs you can supply a volume to the docker run command:
```
docker run -d -p 10242:10242 -p 10243:10243 -v /tmp/datastore:/bc/datastore bigconnect/bigconnect
```
Now, open a browser and go to [http://localhost:10243/cypherlab/index.html](http://localhost:10243/cypherlab/index.html)

Login with the following details:
- Connect URL: **bolt://localhost:10242** 
- Username: **admin**
- Password **admin**

### Locally
To run the latest build you need to build the project first. You need to have Maven installed and JDK11.
```
mvn -Pbin-release clean install
```
The release artifacts are available in the ```release/target/bc-core-*``` folder

### Advanced
Advanced installations require a Hadoop cluster with Apache Accumulo 1.9, RabbitMQ and an ElasticSearch 7.6+ cluster.

## Contributing
Contributions are warmly welcomed and greatly appreciated. Here are a few ways you can contribute:

* Start by fixing some [issues](https://github.com/bigconnect/bigconnect/issues?q=is%3Aissue+is%3Aopen)
* Submit a Pull Request with your fix

## Getting help & Contact
* [Official Forum](https://community.bigconnect.io/)
* [LinkedIn Page](https://www.linkedin.com/company/bigconnectcloud/)

## License
BigConnect is an open source product licensed under AGPLv3.
