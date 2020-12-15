<p align="center">
  <img src="docs/logo.png"/>
  <br>A distributed, scalable, multi-model big data store<br>
</p>

Welcome to BigConnect! [![Build Status](https://travis-ci.com/bigconnect/bigconnect.svg?branch=master)](https://travis-ci.com/bigconnect/bigconnect)
----------------------
[bigconnect.io](https://bigconnect.io)

A multi-model Big Data graph store supporting document, key/value and object models.

It’s an information-agnostic system where all data that flows into the system is transformed, mapped, enriched and then stored in a logical way using a semantic model of concepts, attributes and relationships. 

Extensible, Massively Scalable, Highly secure. Can be tailored to build solutions that cover most data analytics use cases and industry requirements, from financial analysis to social media and cybersecurity. It runs on-premise, in the cloud, or both.

Features:

* Dynamic data model (concepts, relations, properties, tables)
* Multivalued properties, property metadata
* Authorizations and visibilities down to each property value
* Index data for fast search
* Async processing pipeline for applying custom logic on import/update/delete
* Full Cypher query language support
* Super extensible and modular
* Beautiful feature-rich visual explorer
* Developed in Java
* Runs on Apache Accumulo, HDFS and ElasticSearch

## QUERY AND SEARCH
#### FULL-TEXT, FUZZY, SPATIAL, AGGREGATIONS...
Search data using Boolean operators with full-text, spatial, range, fuzzy, and wildcard queries. Supports term, histogram and geohash aggregations. Powered by ElasticSearch.

#### CYPHER
Full support for the Cypher query language with enhanced features like property metadata.

#### SQL
Presto driver for interrogating data using SQL.

## ASYNC DATA PROCESSING
Extract names and sentiment from text, objects and people in images or videos, text from audio data, events from log files or everything you can think of through distributed, asynchronous pipelines.

Examples: 
<table>
<thead>
<th>TEXT</th>
<th>IMAGES/VIDEO</th>
<th>AUDIO</th>
<th>NETWORK</th>
<th>LOGS</th>
<th>CUSTOM</th>
</thead>
<tbody>
<tr>
<td>Entity Extraction</td>
<td>EXIF</td>
<td>Metadata</td>
<td>Protocol identification</td>
<td>Grok field extraction</td>
<td rowspan="4">Build your plugin using any programming language</td>
</tr>
<tr>
<td>Sentiment analysis</td>
<td>OCR</td>
<td>Speech-To-Text</td>
<td>Source, Destination</td>
<td>Correlation</td>
</tr>
<tr>
<td>Classification</td>
<td>Object Detection, Image Captioning</td>
<td>Speaker Identification</td>
<td>Packet contents</td>
<td></td>
</tr>
<tr>
<td>Summarization</td>
<td>Facial Recognition</td>
<td></td>
<td>Correlation</td>
<td></td>
</tr>
</tbody>
</table>

## SECURITY
At the data level, fine-grained access can be controlled up to an object's attributes and only users with specific authorizations can access the data.

## DATA EXPLORER

Visual tool dedicated to data discovery and exploration supporting data ingestion, mapping, enrichment, enterprise search, link analysis, spatial analysis and more. 
