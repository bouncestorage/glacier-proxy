# glacier-proxy

A proxy that presents an Amazon Glacier interface on one side and can backend onto different storage prodivers on the
other. The goal is to ease the pain of developing applications for Amazon Glacier by allowing them to be tested against
a local storage backend.

### Usage

Glacier proxy requires maven. Once maven is setup, you can create the Glacier Proxy jar with
```
mvn package
```

After this, the jar should be available at ```./target/glacier-proxy-1.0-SNAPSHOT-jar-with-dependencies.jar```.

The proxy can be launched with ```java -jar ./target/glacier-proxy-1.0-SNAPSHOT-jar-with-dependencies.jar```.

#### Tests
Running the [glacier tests](https://github.com/bouncestorage/glacier-tests):
```
./src/test/resources/run-glacier-tests.sh
```

Make sure to initialize the submodules (```git submodule init```) and keep them up to date
(```git submodule update```). After initializing the submodule, run ```./bootstrap``` in the submodule directory.

### TODO
- add support for other backends, rather than just in-memory
- add support for additional configuration options (such as port, provider, credentials)
- authenticate requests
- validate tree- and SHA256 hashes in requests

### License
Copyright (C) 2015-2016 Bounce Storage

Licensed under the Apache License, Version 2.0
