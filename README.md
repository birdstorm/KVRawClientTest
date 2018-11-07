# KVRawClientTest
A Test for TiKV's Java Client

First, build tikv-client by `mvn clean install -Dmaven.test.skip=true` using branch of https://github.com/pingcap/tispark/tree/add_raw_client

Then run KVRawClientTest by `mvn clean install exec:java`
