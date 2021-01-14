<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at
  http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# LDAP Transformation Plugin

## Description


## Input

See src/main/doc/ldapinput.adoc

## Output

See src/main/doc/ldapputput.adoc


## Developer Resource

### Setting up local ldap

```console
$ docker run -h localhost -p 1389:1389 -p 1636:1636 -p 4444:4444 --name ldap-01 openidentityplatform/opendj
```

### Preparing for LDAPS test

```
$ openssl s_client -showcerts -connect localhost:1636 </dev/null 2>&1 | sed -ne '/-BEGIN CERTIFICATE-/,/-END CERTIFICATE-/p' > opendj.crt
$ keytool -importcert -file opendj.crt -alias opendj -keystore self-signed.truststore
Enter keystore password:  changeit
Re-enter new password: changeit
Owner: CN=localhost, O=OpenDJ RSA Self-Signed Certificate
Issuer: CN=localhost, O=OpenDJ RSA Self-Signed Certificate
Serial number: 1a7469aa
Valid from: Tue Oct 06 21:29:41 EDT 2020 until: Mon Oct 01 21:29:41 EDT 2040
Certificate fingerprints:
	SHA1: B3:67:7D:BE:10:13:63:38:BF:D3:2D:4C:27:F7:CD:4A:86:66:34:AC
	SHA256: 3E:FD:D1:1F:E4:24:9F:BF:4B:C1:BA:4E:38:FF:06:0D:37:CF:AF:A1:85:12:B1:A0:28:C1:62:8F:C6:10:EC:DC
Signature algorithm name: SHA256withRSA
Subject Public Key Algorithm: 2048-bit RSA key
Version: 3
Trust this certificate? [no]:  yes
Certificate was added to keystore


$ keytool -list -keystore self-signed.truststore
```


### Testing

Enable JUnit test cases by uncommenting @Test annotation (TODO: Use JUnit include and exclude option so that these test case is not running in CI/CD (Jenkins)
