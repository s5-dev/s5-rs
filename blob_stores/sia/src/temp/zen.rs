use std::{collections::HashMap, sync::LazyLock};

pub fn get_address_for_hostkey(key: &str) -> Option<String> {
    HOSTKEY_TO_ADDRESS_MAP.get(key).map(|s| s.to_string())
}

static HOSTKEY_TO_ADDRESS_MAP: LazyLock<HashMap<&str, &str>> = LazyLock::new(|| {
    let mut map = HashMap::new();

    map.insert(
        "ed25519:707d189dfd08469efebf1a51338b3d73eed4ac1b56213a0ed6521179f60a7b71",
        "hostd-test.howitts.co.uk:9884",
    );
    map.insert(
        "ed25519:09a5972a8fe871f765a52729fab15a78a2e0a7d85a8b614de3f4295f2a87292b",
        "zen-01.sia-storage.net:9884",
    );
    map.insert(
        "ed25519:ef6e8ee81420e925e093d795d82cf876cd183adef8cad20f4a0abf42d1621e78",
        "io1.siacoin.rocks:9984",
    );
    map.insert(
        "ed25519:69e9528f18046bf967b64963f72daf3aa72e70324cf108d4392cbed9bb6df21d",
        "hostd.zen.sia5.net:9984",
    );
    map.insert(
        "ed25519:607f64955fa347a0a965570af509348b346b55f6427432d394e0ee1191bce790",
        "hostd-dev.siacoin.rocks:19884",
    );

    // ! below are manually resolved
    map.insert(
        "ed25519:075c746dd0e85eea7aeb05a5c4f37c2af35e4a8c48493fccec285ead4dbe5c49",
        "0te78regt1fekunb0mis9srs5bplsikc914jvj7c51faqjdubh4g.sia.host:9884",
    );
    map.insert(
        "ed25519:1d971572479f230f446f88a05970cc0377daf5b504fd4862ce05d882d6851727",
        "3mbhasi7jshguh3fh2g5is6c0drtltdl0jukgome0nc85lk52sjg.sia.host:59984",
    );
    map.insert(
        "ed25519:5ca24e23ce9aab59ba95d0fb5901be3c435ded7fac9ab40c21e6bad01b9d5afe",
        "bih4s8uejalljeklq3tli0du7h1lrrbvlidb8311sqtd06stbbv0.sia.host:9984",
    );
    map.insert(
        "ed25519:6fdce0663adbff9dd0ced4360721c8aea53433cab61c72c2f0082d3a87085400",
        "dvee0phqrfvprk6eqgr0e8e8lqij8cuamoe75gng10mjl1o8ag00.sia.host:9884",
    );
    map.insert(
        "ed25519:84956180b91ee359c1792064d7b671bf6c49db8dc6eea80965dfb31904b0b520",
        "giam305p3rhljgbp41idfdjhntm4jmsdornag2b5ruphi15gmkg0.sia.host:9884",
    );
    map.insert(
        "ed25519:87e90594ac909076f6473fbebfe278fa6794fe0bb7b3b093b0792e5a5b734239",
        "gvkgb55ci287dti77uvbvojov9jp9vgbmupr14tgf4n5kmrj88sg.sia.host:9810",
    );
    map.insert(
        "ed25519:95770d3b9bf02e025bb7f76715cccf943974e73c480d81e1d9b480845bca1989",
        "ilrgqesru0n04mtnutjhbj6figsn9pps906o3oepmi088mua364g.sia.host:9884",
    );
    map.insert(
        "ed25519:a3aef7db34413e0f1d33140c3cab0a0113317d55b99df6c77a38fbc7a11d2ae1",
        "kenffmpk84v0u79j2g63paoa049j2valn6evdhrq73tsf88t5bgg.sia.host:9884",
    );
    map.insert(
        "ed25519:a95bfd42307179f0affc1512b7014f0c50ab4d208d02f8e18d2ead8bf9b08edb",
        "l5dvqghge5sv1bvs2k9be0af1h8amj90hk1fhocd5qmonudghrdg.sia.host:9884",
    );
    map.insert(
        "ed25519:ee5d52961e3fb132b3b1c29efac39ad0d916a73e14663ebc5b90e22322039e25",
        "mb32q8167orgrmcc6n0ndlrl897jafio0oks881dkal4qd4eho1g.sia.host:9984",
    );
    map.insert(
        "ed25519:de43e12e37bb3662caeb3a78e7359281aa5c8cf1f960c88792cea393cd4410fc",
        "rp1u2bhnncr65inb79seedcig6l5p37hv5gch1sipqhp7ja423u0.sia.host:9884",
    );
    map.insert(
        "ed25519:e3e710c1ea553729ef932b17fbec98e1e6acba484797624d06fb275a10f886da",
        "sfjh1gfaakrijrsj5cbvnr4os7japei88ubm4j86vcjlk47ogrd0.sia.host:9884",
    );
    map
});
