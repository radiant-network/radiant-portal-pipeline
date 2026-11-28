# Changelog

## [1.3.0](https://github.com/radiant-network/radiant-portal-pipeline/compare/v1.2.0...v1.3.0) (2026-11-28)


### Features

* RAD-272 read the somatic CNV on the tumor-only alignment task ([#374](https://github.com/radiant-network/radiant-portal-pipeline/issues/374)) ([595659e](https://github.com/radiant-network/radiant-portal-pipeline/commit/595659e5a5922ca1a175258b5c269dffa5a1dca5))
* RAD-288 each pipeline decides and moves its cases and the import hands them to a case evaluation dag ([#382](https://github.com/radiant-network/radiant-portal-pipeline/issues/382)) ([f280f4b](https://github.com/radiant-network/radiant-portal-pipeline/commit/f280f4b530cc22090bdc52fb1f1ca69f899f02e4))
* RAD-288 germline SNV runs the cases the scheduler decided, a missing parent no longer blocks ([#362](https://github.com/radiant-network/radiant-portal-pipeline/issues/362)) ([26910c4](https://github.com/radiant-network/radiant-portal-pipeline/commit/26910c4dc584cea0009edc95fd2779a7c83709e3))
* RAD-288 the case scheduler journals its germline CNV decisions and the CNV post-processing runs the cases it decided ([#372](https://github.com/radiant-network/radiant-portal-pipeline/issues/372)) ([cfab51a](https://github.com/radiant-network/radiant-portal-pipeline/commit/cfab51aead36bbe690536cca3456529e81c96f89))
* RAD-288 the case scheduler journals its germline SNV decisions ([#361](https://github.com/radiant-network/radiant-portal-pipeline/issues/361)) ([281a39c](https://github.com/radiant-network/radiant-portal-pipeline/commit/281a39cbe4d64dea3f2888d5143a5f3b13a3714e))
* RAD-288 the case scheduler journals its quality control decisions and the qc dag runs the cases it decided ([#375](https://github.com/radiant-network/radiant-portal-pipeline/issues/375)) ([147075b](https://github.com/radiant-network/radiant-portal-pipeline/commit/147075b8404584ca90ebf1277abb5a4333a9d94e))
* RAD-288 the case scheduler journals its somatic snv decisions per calling task and the somatic dag runs the tasks it decided ([#378](https://github.com/radiant-network/radiant-portal-pipeline/issues/378)) ([03d60bb](https://github.com/radiant-network/radiant-portal-pipeline/commit/03d60bb9c86bdb942696d8dc15e1fc4104c4a669))
* RAD-288 the germline SNV post-processing journals succeeded or failed per case ([#363](https://github.com/radiant-network/radiant-portal-pipeline/issues/363)) ([1d6fa8a](https://github.com/radiant-network/radiant-portal-pipeline/commit/1d6fa8a6cdde7e0b4c9ba24ca46417924e0134d1))
* RAD-288 the import decides its tasks in python from sql facts and journals them ([#381](https://github.com/radiant-network/radiant-portal-pipeline/issues/381)) ([6eb4de3](https://github.com/radiant-network/radiant-portal-pipeline/commit/6eb4de34e137195e184c47273f0343fba31be524))
* RAD-288 the pipeline journal table and its write rule ([#360](https://github.com/radiant-network/radiant-portal-pipeline/issues/360)) ([55f8c30](https://github.com/radiant-network/radiant-portal-pipeline/commit/55f8c3033e226efc13802cb0353e00252a2c2aa3))
* rad-369 import gene coverage ([#367](https://github.com/radiant-network/radiant-portal-pipeline/issues/367)) ([2973fe1](https://github.com/radiant-network/radiant-portal-pipeline/commit/2973fe13dbd502ca892b2bd01c1fb2ca0aebf164))
* rad-378 batch the exomiser load ([#370](https://github.com/radiant-network/radiant-portal-pipeline/issues/370)) ([e1ffd98](https://github.com/radiant-network/radiant-portal-pipeline/commit/e1ffd98ad54480adbfb6e5342d68ff5c48907d78))
* RAD-434 add opt-in Rust engine for parsing SNV VCFs ([#379](https://github.com/radiant-network/radiant-portal-pipeline/issues/379)) ([69c8b95](https://github.com/radiant-network/radiant-portal-pipeline/commit/69c8b958e5df76be26379b0473f827de3ed81d6b))


### Bug Fixes

* rad-377 rely on created_on only for calculating what to process ([#369](https://github.com/radiant-network/radiant-portal-pipeline/issues/369)) ([c8fb06f](https://github.com/radiant-network/radiant-portal-pipeline/commit/c8fb06fb98fbc995e7278d3aa9dfdfd12e3804df))
* RAD-399 assign partitions per group of linked rows ([#376](https://github.com/radiant-network/radiant-portal-pipeline/issues/376)) ([0432027](https://github.com/radiant-network/radiant-portal-pipeline/commit/04320278a2cb5ad24673dda125edce9ede85274c))
* RAD-400 purge the snv rows of deleted tasks ([#380](https://github.com/radiant-network/radiant-portal-pipeline/issues/380)) ([d07b182](https://github.com/radiant-network/radiant-portal-pipeline/commit/d07b18222a5169f3ba1df4cd5900779ae01c1a79))

## [1.2.0](https://github.com/radiant-network/radiant-portal-pipeline/compare/v1.1.32...v1.2.0) (2026-11-18)


### Features

* **qc:** RAD-180 register the per-sample qc metrics json ([#332](https://github.com/radiant-network/radiant-portal-pipeline/issues/332)) ([d2161cb](https://github.com/radiant-network/radiant-portal-pipeline/commit/d2161cb0cb1b48dd9d1c1472408c6f6ecc0366da))
* **qc:** RAD-214 run the qc pipeline on the tumor of somatic cases ([#336](https://github.com/radiant-network/radiant-portal-pipeline/issues/336)) ([4731e50](https://github.com/radiant-network/radiant-portal-pipeline/commit/4731e50e2777f23566684a1a21a96074386a6d46))
* RAD-169 bump cnv-post-processing to main 6e3be8d, register the proband-named slivar VCF ([#364](https://github.com/radiant-network/radiant-portal-pipeline/issues/364)) ([998bb4f](https://github.com/radiant-network/radiant-portal-pipeline/commit/998bb4fc3fed2b25b27d675d6834d22cf7e7886c))
* rad-186 implement upgrade notes ([#352](https://github.com/radiant-network/radiant-portal-pipeline/issues/352)) ([26b22e4](https://github.com/radiant-network/radiant-portal-pipeline/commit/26b22e4292d1a6048f3d534885ae0e0b252040b6))
* RAD-194 store the gnomAD SV allele count on CNV occurrences ([#327](https://github.com/radiant-network/radiant-portal-pipeline/issues/327)) ([29ae33d](https://github.com/radiant-network/radiant-portal-pipeline/commit/29ae33def272c49a76bdc6b7762c33444acaddbd))
* rad-200 add conventional commit validation ([#328](https://github.com/radiant-network/radiant-portal-pipeline/issues/328)) ([31a3ed9](https://github.com/radiant-network/radiant-portal-pipeline/commit/31a3ed96354be458c37a95a13fc74519f92711d7))
* rad-201 add release gate ([#339](https://github.com/radiant-network/radiant-portal-pipeline/issues/339)) ([58d4a1b](https://github.com/radiant-network/radiant-portal-pipeline/commit/58d4a1b43ad5c0a9f2e0faa57aa1340b719dc652))
* rad-202 add automatic release pr and tag ([#341](https://github.com/radiant-network/radiant-portal-pipeline/issues/341)) ([f26219f](https://github.com/radiant-network/radiant-portal-pipeline/commit/f26219fac37e242ebfebbbb5868a6516aecb64b8))
* rad-202 use appropriate release-please app ([#342](https://github.com/radiant-network/radiant-portal-pipeline/issues/342)) ([fbef8cf](https://github.com/radiant-network/radiant-portal-pipeline/commit/fbef8cf780b7f5e2f96a656b77e77fdefeb02104))
* rad-206 handle new output format from cnv-post-processing ([#335](https://github.com/radiant-network/radiant-portal-pipeline/issues/335)) ([2c65829](https://github.com/radiant-network/radiant-portal-pipeline/commit/2c65829a69ebdc1fd576cacccfc89b56bf1c8791))
* rad-206 implement cnv post-processing ([#331](https://github.com/radiant-network/radiant-portal-pipeline/issues/331)) ([5a90c24](https://github.com/radiant-network/radiant-portal-pipeline/commit/5a90c24cfde23ba370f0a04af5def68faabcbd60))
* rad-206 update parsing of the PE field for slivar CNVs ([#348](https://github.com/radiant-network/radiant-portal-pipeline/issues/348)) ([e3dbc1a](https://github.com/radiant-network/radiant-portal-pipeline/commit/e3dbc1ad3a390a4ad2087d8e342c406cdacbf6ae))
* RAD-239 - dockerfile CVE-2026-109518 ([#338](https://github.com/radiant-network/radiant-portal-pipeline/issues/338)) ([b5d1086](https://github.com/radiant-network/radiant-portal-pipeline/commit/b5d1086680e1ace052434be903c9685cc3c1e731))
* RAD-246 build the reference transcript of each gene from ensembl and mane ([#347](https://github.com/radiant-network/radiant-portal-pipeline/issues/347)) ([9a21ec4](https://github.com/radiant-network/radiant-portal-pipeline/commit/9a21ec42a71237b13f36723116abb9a96019a145))
* rad-253 adapt gnomAD to 4.1.2 ([#340](https://github.com/radiant-network/radiant-portal-pipeline/issues/340)) ([291db87](https://github.com/radiant-network/radiant-portal-pipeline/commit/291db871f2410754125de955e625cb854ecc6a06))
* rad-256 Use exomiser output from CNV post processing ([#343](https://github.com/radiant-network/radiant-portal-pipeline/issues/343)) ([5422a03](https://github.com/radiant-network/radiant-portal-pipeline/commit/5422a033fd62ac3d11d340e3f31e064e2d9b4f4f))


### Bug Fixes

* **docker:** RAD-275 upgrade debian packages in the dbt and operator images ([#349](https://github.com/radiant-network/radiant-portal-pipeline/issues/349)) ([3ca8e30](https://github.com/radiant-network/radiant-portal-pipeline/commit/3ca8e3072b72c64fd9bcc728e6504a3540e73c2c))
* RAD-266 keep the germline task types out of somatic cases in the staging view ([#350](https://github.com/radiant-network/radiant-portal-pipeline/issues/350)) ([a53742c](https://github.com/radiant-network/radiant-portal-pipeline/commit/a53742cd2c37cb3331c94c28d95caeaa78d3f608))
* rad-352 remove no somatic check for qlin tenant ([#365](https://github.com/radiant-network/radiant-portal-pipeline/issues/365)) ([cc293c4](https://github.com/radiant-network/radiant-portal-pipeline/commit/cc293c4e3a6cda592596a6201888059f5a32942b))
* rad-352 use probands' FT value in slivar ([#366](https://github.com/radiant-network/radiant-portal-pipeline/issues/366)) ([23dc13b](https://github.com/radiant-network/radiant-portal-pipeline/commit/23dc13b34f2bfbc7ae76e8e24a46b62ffd78b392))
