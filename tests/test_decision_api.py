from hop.io import Metadata
import hop.io
import hop.models
from io import BytesIO
import pytest
import uuid

from archive import database_api
from archive import decision_api
from archive import store_api

pytest_plugins = ('pytest_asyncio',)

@pytest.mark.asyncio
async def test_is_content_identical():
    config = {
        "db_type": "mock",
        "store_type": "mock",
        "store_primary_bucket": "b1",
        "store_backup_bucket": "b2",
	    "store_region_name": "eu-north-3",
	    "text_index_size_limit": 4*1024,
    }
    db = database_api.DbFactory(config)
    st = store_api.StoreFactory(config)
    await db.connect()
    await st.connect()
    
    m1 = (b"abcdefgh",
          Metadata(topic="t1", partition=0, offset=0, timestamp=172, key="", headers=[], _raw=None))
    m2 = (b"abcdefgh",
          Metadata(topic="t1", partition=0, offset=1, timestamp=223, key="", headers=[], _raw=None))
    m3 = (b"somethingelse",
          Metadata(topic="t1", partition=0, offset=2, timestamp=356, key="", headers=[], _raw=None))
    
    with decision_api.Decider({}) as decider:
        async def store(payload, metadata):
            annotations = decider.get_annotations(payload, metadata.headers)
            await st.store(payload, metadata, annotations)
            await db.insert(metadata, annotations)
            return annotations["con_text_uuid"]
    
        is_content_identical = decision_api.is_content_identical
        ids = []
        ids.append(await db.get_message_id(await store(*m1)))
        assert await is_content_identical(ids, db, st), "A single message is identical to itself"
        ids.append(await db.get_message_id(await store(*m2)))
        assert await is_content_identical(ids, db, st), "Messages with the same content are identical"
        ids.append(await db.get_message_id(await store(*m3)))
        assert not await is_content_identical(ids, db, st), "Messages with the different content are not identical"

def test_get_text_uuid():
    with decision_api.Decider({}) as decider:
        ui1 = uuid.uuid4()
        uo1 = decider.get_text_uuid([("_id", ui1.bytes)])
        assert uo1[0] == str(ui1)
        assert uo1[1], "UUID should be marked as originating from the client"
        
        # A raw UUID viewed via a memoryview should be correctly extracted
        buf = BytesIO()
        buf.write(b"prefix")
        buf.write(ui1.bytes)
        buf.write(b"suffix")
        v = memoryview(buf.getvalue())
        uo1v = decider.get_text_uuid([("_id", v[6:22])])
        assert uo1v[0] == str(ui1)
        assert uo1v[1], "UUID should be marked as originating from the client"
        
        ui2 = uuid.uuid4()
        uo2 = decider.get_text_uuid([("_id", ui1.bytes), ("_id", ui2.bytes)])
        assert uo2[0] == str(ui1), "First ID should be selected"
        assert uo2[1], "UUID should be marked as originating from the client"
        
        uo3 = decider.get_text_uuid([("_id", b"not a UUID"), ("_id", ui2.bytes)])
        assert uo3[0] == str(ui2), "Invalid garbage should be skipped"
        assert uo3[1], "UUID should be marked as originating from the client"
        
        uo4 = decider.get_text_uuid([("_id", b"not a UUID")])
        uuid.UUID(hex=uo4[0])
        assert not uo4[1], "UUID should be marked as originating from the server"

def test_get_string_header():
    with decision_api.Decider({}) as d:
        assert d.get_string_header([("foo", b"bar")], "foo") == "bar"
        
        assert d.get_string_header([("foo", b"bar"), ("foo", b"baz")], "foo") == "bar"
        
        assert d.get_string_header([("foo", b"bar"), ("baz", b"quux"), ("xen", b"hom")], "baz") == "quux"
        
        assert d.get_string_header([("foo", b"\x00not\xE0\x01valid\xF0\x10utf8"), 
                                    ("baz", b"quux")], "foo") == ""

voevent_xml = b"""<?xml version='1.0' encoding='UTF-8'?>
<voe:VOEvent xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:voe="http://www.ivoa.net/xml/VOEvent/v2.0" xsi:schemaLocation="http://www.ivoa.net/xml/VOEvent/v2.0 http://www.ivoa.net/xml/VOEvent/VOEvent-v2.0.xsd" version="2.0" role="observation" ivorn="ivo://gwnet/LVC#S200302c-1-Preliminary">
  <Who>
    <Date>2020-03-02T02:00:09</Date>
    <Author>
      <contactName>LIGO Scientific Collaboration and Virgo Collaboration</contactName>
    </Author>
  </Who>
  <What>
    <Param dataType="string" name="AlertType" ucd="meta.version" value="Preliminary">
      <Description>VOEvent alert type</Description>
    </Param>
    <Param dataType="string" name="Instruments" ucd="meta.code" value="H1,V1">
      <Description>List of instruments used in analysis to identify this event</Description>
    </Param>
    <Group name="GW_SKYMAP" type="GW_SKYMAP">
      <Param dataType="string" name="skymap_fits" ucd="meta.ref.url" value="https://gracedb.ligo.org/api/superevents/S200302c/files/bayestar.fits.gz,0">
        <Description>Sky Map FITS</Description>
      </Param>
    </Group>
    <Group name="Properties" type="Properties">
      <Param dataType="float" name="HasNS" ucd="stat.probability" value="0.0">
        <Description>Probability that at least one object in the binary has a mass that is less than 3 solar masses</Description>
      </Param>
      <Param dataType="float" name="HasRemnant" ucd="stat.probability" value="0.0">
        <Description>Probability that a nonzero mass was ejected outside the central remnant object</Description>
      </Param>
    </Group>
  </What>
  <WhereWhen>
    <ObsDataLocation>
      <ObservationLocation>
        <AstroCoordSystem id="UTC-FK5-GEO"/>
        <AstroCoords coord_system_id="UTC-FK5-GEO">
          <Time unit="s">
            <TimeInstant>
              <ISOTime>2020-03-02T01:58:11.519119</ISOTime>
            </TimeInstant>
          </Time>
        </AstroCoords>
      </ObservationLocation>
    </ObsDataLocation>
  </WhereWhen>
  <Description>Report of a candidate gravitational wave event</Description>
  <How>
    <Description>Candidate gravitational wave event identified by low-latency analysis</Description>
    <Description>V1: Virgo 3 km gravitational wave detector</Description>
    <Description>H1: LIGO Hanford 4 km gravitational wave detector</Description>
  </How>
</voe:VOEvent>
"""

def test_get_data_format():
    with decision_api.Decider({}) as d:
        voevent = hop.models.VOEvent.load(voevent_xml)
        m, h = hop.io.Producer.pack(voevent)
        detected = d.get_data_format(m, h)
        assert detected == "application/x.voevent"
        
        gcntextnotice = hop.models.GCNTextNotice.load(
            b'TITLE:            GCN/AMON NOTICE\n'
            b'NOTICE_DATE:      Fri 12 Apr 24 05:34:35 UT\n'
            b'NOTICE_TYPE:      ICECUBE Astrotrack Bronze \n'
            b'SRC_RA:           103.7861d {+06h 55m 09s} (J2000),\n'
            b'SRC_DEC:           +5.8716d {+05d 52\' 18"} (J2000),\n'
            b'DISCOVERY_DATE:   20412 TJD;   103 DOY;   24/04/12 (yy/mm/dd)\n'
            b'DISCOVERY_TIME:   20026 SOD {05:33:46.89} UT\n'
            b'ENERGY:           1.2126e+02 [TeV]\n'
        )
        m, h = hop.io.Producer.pack(gcntextnotice)
        detected = d.get_data_format(m, h)
        assert detected == "application/x.gcn.notice"
        
        gcncircular = hop.models.GCNCircular(
            header={
                "title": "GCN GRB OBSERVATION REPORT",
                "number": "40",
                "subject": "GRB980329 VLA observations",
                "date": "98/04/03 07:10:15 GMT",
                "from": "Greg Taylor at NRAO",
            },
            body="""
                The flux density measurements of VLA J0702+3850 are as follows:
                Date(UT)   8.4 GHz Flux Density
                --------   ----------------------
                Mar 30.2   166 +/- 50 microJy
                Apr  1.1   248 +/- 16    "
                Apr  2.1    65 +/- 25    "
                """
        )
        m, h = hop.io.Producer.pack(gcncircular)
        detected = d.get_data_format(m, h)
        assert detected == "application/x.gcn.circular"
        
        jsonblob = hop.models.JSONBlob({"foo": "bar", 
                                        "baz":{"quux": [1, 2, 3], "xen": False, "hom": 3.6}})
        m, h = hop.io.Producer.pack(jsonblob)
        detected = d.get_data_format(m, h)
        assert detected == "application/json"
        
        avroblob = hop.models.AvroBlob({"foo": "bar", 
                                        "baz":{"quux": [1, 2, 3], "xen": False, "hom": 3.6,
                                               "drel": b"\x00\x01\x02\x03"}})
        m, h = hop.io.Producer.pack(avroblob)
        detected = d.get_data_format(m, h)
        assert detected == "application/avro"
        
        # Blob can be many things
        asciiblob = hop.models.Blob(b"Sphinx of black quartz, hear my vow!")
        m, h = hop.io.Producer.pack(asciiblob)
        detected = d.get_data_format(m, h)
        assert detected == "text/plain"
        
        pngblob = hop.models.Blob(b"\x89PNG\x0D\x0A\x1A\x0A\x00\x00\x00\x0DIHDR\x00\x00\x00\x01"
                                  b"\x00\x00\x00\x01\x01\x00\x00\x00\x00\x37\x6E\xF9"
                                  b"\x24\x00\x00\x00\x0A\x49\x44\x41\x54\x78\x01\x63"
                                  b"\x60\x00\x00\x00\x02\x00\x01\x73\x75\x01\x18\x00"
                                  b"\x00\x00\x00IEND\xAE\x42\x60\x82")
        m, h = hop.io.Producer.pack(pngblob)
        detected = d.get_data_format(m, h)
        assert detected == "image/png"
        
        longblob = hop.models.Blob(8192*b"\x00")
        m, h = hop.io.Producer.pack(longblob)
        detected = d.get_data_format(m, h)
        assert detected == "application/octet-stream"

def test_get_annotations():
    with decision_api.Decider({}) as decider:
        m1 = (b"abcdefgh",[])
        a1 = decider.get_annotations(m1[0], m1[1])
        assert "con_text_uuid" in a1
        uuid.UUID(a1["con_text_uuid"])
        assert "con_is_client_uuid" in a1
        assert not a1["con_is_client_uuid"]
        assert "con_message_crc32" in a1
        
        u = uuid.UUID("01234567-aaaa-bbbb-cccc-0123456789de")
        m2 = (b"0123456789",[("_id",u.bytes)])
        a2 = decider.get_annotations(m2[0], m2[1])
        assert "con_text_uuid" in a2
        assert uuid.UUID(a2["con_text_uuid"]) == u
        assert "con_is_client_uuid" in a2
        assert a2["con_is_client_uuid"]
        assert "con_message_crc32" in a2

@pytest.mark.asyncio
async def test_is_deemed_duplicate():
    config = {
        "db_type": "mock",
        "store_type": "mock",
        "store_primary_bucket": "b1",
        "store_backup_bucket": "b2",
        "store_region_name": "r",
    }
    db = database_api.DbFactory(config)
    st = store_api.StoreFactory(config)
    await db.connect()
    await st.connect()
    with decision_api.Decider(config) as decider:
        
        m1 = (b"abcdefgh",
              Metadata(topic="t1", partition=0, offset=0, timestamp=172, key="", headers=[], _raw=None))
        m2 = (b"abcdefgh",
              Metadata(topic="t1", partition=0, offset=1, timestamp=172, key="", headers=[], _raw=None))
        u = uuid.UUID("01234567-aaaa-bbbb-cccc-0123456789de")
        m3 = (b"somethingelse",
              Metadata(topic="t1", partition=0, offset=2, timestamp=356, key="", headers=[("_id",u.bytes)], _raw=None))
        m4 = (b"clienttriestoreuseuuid",
              Metadata(topic="t1", partition=0, offset=2, timestamp=356, key="", headers=[("_id",u.bytes)], _raw=None))
        
        async def store(payload, metadata, annotations):
            await st.store(payload, metadata, annotations)
            await db.insert(metadata, annotations)
            return annotations["con_text_uuid"]
        
        a1 = decider.get_annotations(m1[0], m1[1].headers)
        assert not await decider.is_deemed_duplicate(a1, m1[1], db, st), "With no messages stored, a message cannot be a duplicate"
        await store(m1[0], m1[1], a1)
        
        a2 = decider.get_annotations(m2[0], m2[1].headers)
        assert await decider.is_deemed_duplicate(a2, m2[1], db, st), "A message with the same body, timestamp, and topic is a duplicate"
        
        a3 = decider.get_annotations(m3[0], m3[1].headers)
        assert not await decider.is_deemed_duplicate(a3, m3[1], db, st), "A message with a new UUID is not a duplicate"
        await store(m3[0], m3[1], a3)
        
        assert await decider.is_deemed_duplicate(a3, m3[1], db, st), "The same message again is a duplicate"
        
        a4 = decider.get_annotations(m4[0], m4[1].headers)
        assert a4["con_text_uuid"] == str(u)
        assert a4["con_is_client_uuid"]
        assert not await decider.is_deemed_duplicate(a4, m4[1], db, st), "A message with a new UUID is not a duplicate"
        assert a4["con_text_uuid"] != str(u)
        assert not a4["con_is_client_uuid"]
        await store(m4[0], m4[1], a4)

@pytest.mark.asyncio
async def test_get_indexable_text_from_headers():
    u = uuid.uuid4()
    with decision_api.Decider({}) as d:
        m = b"datadatadata"
        h = [("title", b"The Unstrung Harp"), ("_sender", b"egorey-917a2d4c"), ("_id", u.bytes)]
        a = d.get_annotations(m, h)
        text = d.get_indexable_text(m, h, a)
        assert "title" in text
        assert "The Unstrung Harp" in text
        assert "sender" in text
        assert "egorey" in text
        assert "id" not in text
        
        m = b"datadatadata"
        h = [("binary", b"\x00not\xE0\x01valid\xF0\x10utf8"), ("_id", u.bytes)]
        a = d.get_annotations(m, h)
        text = d.get_indexable_text(m, h, a)
        assert "binary" not in text
        assert "valid" not in text
        assert "id" not in text

@pytest.mark.asyncio
async def test_get_indexable_text_message():
    config = {
        "text_index_message_size_limit": 1<<20,
        "text_index_size_limit": 1<<12,
    }
    with decision_api.Decider(config) as d:
        m = b"datadatadata"
        h = []
        a = d.get_annotations(m, h)
        text = d.get_indexable_text(m, h, a)
        assert "datadatadata" in text
        
        m = 2048 * b"abc "
        h = []
        a = d.get_annotations(m, h)
        text = d.get_indexable_text(m, h, a)
        assert len(text) == config["text_index_size_limit"]
        assert "abc abc abc" in text
        
        m = b"U+4W7m+XlVvlifYX1/nW4b42itr183agMHTEIvMotv6bLIBomHyJZkL7AfCCcG/" \
            b"lgBXqSJmQTK0kcZThCCz2gt65M4xZFbFnV2dX9TKsRQws9rDnBaX5VBzSvJvZK5" \
            b"BEYBwVQtN9v03qtMdlzb6PgEKI0cVDmFC6EqVnFeg3Ae8="
        h = []
        a = d.get_annotations(m, h)
        text = d.get_indexable_text(m, h, a)
        assert len(text) == 0, "long, high-entropy strings should not be indexed"
        
        m = b"not\xE0\x01valid\xF0\x10utf8"
        h = []
        a = d.get_annotations(m, h)
        # libmagic is too canny to be fooled by our invalid UTF-8; override it to test other defenses
        a["media_type"] = "text/plain"
        text = d.get_indexable_text(m, h, a)
        assert len(text) == 0, "Non-UTF-8 data should not be indexed"
        
        jsonblob = hop.models.JSONBlob({"foo": "bar", 
                                        "baz":{"quux": [1, "some text", 3], 
                                               "xen": False, "hom": 3.6}})
        m, h = hop.io.Producer.pack(jsonblob)
        a = d.get_annotations(m, h)
        text = d.get_indexable_text(m, h, a)
        assert "foo" in text
        assert "bar" in text
        assert "some text" in text
        assert "hom" in text
        assert "3.6" in text
        
        avroblob = hop.models.AvroBlob({"foo": "bar", 
                                        "baz":{"quux": [1, "some text", 3], 
                                               "xen": False, "hom": 3.6,
                                               "drel": b"Actually a valid UTF-8 string"}})
        m, h = hop.io.Producer.pack(avroblob)
        a = d.get_annotations(m, h)
        text = d.get_indexable_text(m, h, a)
        assert "foo" in text
        assert "bar" in text
        assert "some text" in text
        assert "hom" in text
        assert "3.6" in text

        
        
        gcncircular = hop.models.GCNCircular(
            header={
                "title": "GCN GRB OBSERVATION REPORT",
                "number": "40",
                "subject": "GRB980329 VLA observations",
                "date": "98/04/03 07:10:15 GMT",
                "from": "Greg Taylor at NRAO",
            },
            body="""
                The flux density measurements of VLA J0702+3850 are as follows:
                Date(UT)   8.4 GHz Flux Density
                --------   ----------------------
                Mar 30.2   166 +/- 50 microJy
                Apr  1.1   248 +/- 16    "
                Apr  2.1    65 +/- 25    "
                """
        )
        m, h = hop.io.Producer.pack(gcncircular)
        a = d.get_annotations(m, h)
        text = d.get_indexable_text(m, h, a)
        for key, value in gcncircular.header.items():
            assert key in text
            assert value in text
        assert gcncircular.body in text
        
        gcntextnotice = hop.models.GCNTextNotice.load(
            b'TITLE:            GCN/AMON NOTICE\n'
            b'NOTICE_DATE:      Fri 12 Apr 24 05:34:35 UT\n'
            b'NOTICE_TYPE:      ICECUBE Astrotrack Bronze \n'
            b'SRC_RA:           103.7861d {+06h 55m 09s} (J2000),\n'
            b'SRC_DEC:           +5.8716d {+05d 52\' 18"} (J2000),\n'
            b'DISCOVERY_DATE:   20412 TJD;   103 DOY;   24/04/12 (yy/mm/dd)\n'
            b'DISCOVERY_TIME:   20026 SOD {05:33:46.89} UT\n'
            b'ENERGY:           1.2126e+02 [TeV]\n'
        )
        m, h = hop.io.Producer.pack(gcntextnotice)
        a = d.get_annotations(m, h)
        text = d.get_indexable_text(m, h, a)
        for field in ["TITLE", "NOTICE_DATE", "NOTICE_TYPE", "SRC_RA", "SRC_DEC", "DISCOVERY_DATE",
                      "DISCOVERY_TIME", "ENERGY"]:
            assert field.lower() in text
        for value in ["GCN/AMON NOTICE", "ICECUBE Astrotrack Bronze", "103.7861d {+06h 55m 09s} (J2000)"]:
            assert value in text
        
        voevent = hop.models.VOEvent.load(voevent_xml)
        m, h = hop.io.Producer.pack(voevent)
        a = d.get_annotations(m, h)
        text = d.get_indexable_text(m, h, a)
        print(text)
        assert "LIGO" in text
        assert "Virgo" in text
        assert "H1,V1" in text
        assert "https://gracedb.ligo.org/api/superevents/S200302c/files/bayestar.fits.gz,0" in text
        assert "2020-03-02T01:58:11.519119" in text
