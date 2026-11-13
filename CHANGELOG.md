# Changelog

## [1.2.0](https://github.com/radiant-network/radiant-portal-pipeline/compare/v1.1.32...v1.2.0) (2026-11-13)


### Features

* **qc:** RAD-180 register the per-sample qc metrics json ([#332](https://github.com/radiant-network/radiant-portal-pipeline/issues/332)) ([d2161cb](https://github.com/radiant-network/radiant-portal-pipeline/commit/d2161cb0cb1b48dd9d1c1472408c6f6ecc0366da))
* **qc:** RAD-214 run the qc pipeline on the tumor of somatic cases ([#336](https://github.com/radiant-network/radiant-portal-pipeline/issues/336)) ([4731e50](https://github.com/radiant-network/radiant-portal-pipeline/commit/4731e50e2777f23566684a1a21a96074386a6d46))
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
