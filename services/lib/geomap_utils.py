import math, re, sys
from os import path
from humps import decamelize

from bycon import BYC_PARS, prdbug, test_truthy

services_lib_path = path.join( path.dirname( path.abspath(__file__) ) )
sys.path.append( services_lib_path )
from file_utils import read_www_tsv_to_dictlist

################################################################################

def read_geomarker_table_web(byc):
    geolocs = []
    f_a = BYC_PARS.get("inputfile", "")
    if not "http" in f_a:
        return geolocs
    lf, fieldnames = read_www_tsv_to_dictlist(f_a)

    markers = {}

    for line in lf:
        
        group_lon = line.get("group_lon", "")       # could use 0 here for Null Island...
        group_lat = line.get("group_lat", "")       # could use 0 here for Null Island...
        group_label = line.get("group_label", "")
        item_size = line.get("item_size", "")
        item_label = line.get("item_label", "")
        item_link = line.get("item_link", "")

        if not re.match(r'^\-?\d+?(?:\.\d+?)?$', str(group_lat) ):
            continue
        if not re.match(r'^\-?\d+?(?:\.\d+?)?$', str(group_lon) ):
            continue
        if not re.match(r'^\d+?(?:\.\d+?)?$', str(item_size) ):
            item_size = 1

        m_k = f'{group_label}::LatLon::{group_lat}::{group_lon}'

        # TODO: load schema for this
        if not m_k in markers.keys():
            markers[m_k] = {
                "geo_location": {
                    "type": "Feature",
                    "geometry": {
                        "type": "Point",
                        "coordinates": [ float(group_lon), float(group_lat) ]
                    },
                    "properties": {
                        "city": None,
                        "country": None,
                        "label": group_label,
                        "marker_type": line.get("marker_type", "circle"),
                        "marker_icon": line.get("marker_icon", ""),
                        "marker_count": 0,
                        "items": []
                    }
                }
            }

        g_l_p = markers[m_k]["geo_location"]["properties"]
        g_l_p["marker_count"] += float(item_size)

        if len(item_label) > 0:
            if "http" in item_link:
                item_label = "<a href='{}' target='_blank'>{}</a>".format(item_link, item_label)
            g_l_p["items"].append(item_label)

    for m_k, m_v in markers.items():
        geolocs.append(m_v)

    return geolocs

################################################################################

def print_map_from_geolocations(byc, geolocs_db_results):
    output = BYC_PARS.get("output", "___none___")
    if not "map" in output:
        return

    geolocs = [x["geo_location"] for x in geolocs_db_results if "geo_location" in x]
    m_p = byc["geoloc_definitions"].get("map_params", {})
    p_p = __update_geo_plot_params_from_form(byc)
    m_max_count = __marker_max_from_geo_locations(geolocs)
    
    leaf_markers = []
    for geoloc in geolocs:
        leaf_markers.append( __map_marker_from_geo_location(byc, geoloc, p_p, m_max_count) )
    markersJS = __create_geo__marker_layer(leaf_markers)

    geoMap = """
<!-- map needs to exist before we load leaflet -->
{}
<div id="{}" style="width: {}px; height: {}px;"></div>

<!-- Make sure you put this AFTER Leaflet's CSS -->
<script src="https://unpkg.com/leaflet@1.8.0/dist/leaflet.js"
      integrity="sha512-BB3hKbKWOc9Ez/TAwyWxNXeoV9c1v6FIeYiBieIWkpLjauysF18NzgR1MBNBXf8/KABdlkX68nAhlwcDFLGPCQ=="
      crossorigin=""></script>
<script>
  var circleOptions = {{
    color: '{}',
    stroke: true,
    weight: {},
    fillColor: '{}',
    fillOpacity: {},
    radius: 1000,
    count: 1
  }};

  // Create the map.

  var map = L.map('{}', {{ renderer: L.svg() }} ).setView([{}, {}], {});

  L.tileLayer('{}', {{
      minZoom: {},
      maxZoom: {},
      attribution: '{}'
  }}).addTo(map);

{}

</script>
    """.format(
        m_p.get("head"),
        m_p.get("plotid"),
        p_p.get("map_w_px"),
        p_p.get("map_h_px"),
        p_p["bubble_stroke_color"],
        p_p["bubble_stroke_weight"],
        p_p["bubble_fill_color"],
        p_p["bubble_opacity"],
        m_p.get("plotid"),
        m_p.get("init_latitude"),
        m_p.get("init_longitude"),
        m_p.get("zoom"),
        m_p.get("tiles_source"),
        p_p.get("zoom_min"),
        p_p.get("zoom_max"),
        m_p.get("attribution"),
        markersJS
    )

    if test_truthy(BYC_PARS.get("show_help", False)):
        t = """
<h4>Map Configuration</h4>
<p>The following parameters may be modified by providing alternative values in
the `plotPars` parameter in the URL, e.g. "&plotPars=map_w_px=1024::init_latitude=8.4".

For information about the special parameter format please see http://byconaut.progenetix.org
</p>
<table>
"""
        t += "<tr><th>Map Parameter</th><th>Value</th></tr>\n"
        for p_p_k, p_p_v in p_p.items():
            if not '<' in str(p_p_v):
                t += f'<tr><td>{p_p_k}</td><td>{p_p_v}</td></tr>\n'
        t += "\n</table>"
        geoMap += t

    print("""
<html>
{}
</html>""".format(geoMap))
    exit()


################################################################################

def __create_geo__marker_layer(leaf_markers):
    markersJS = ""
    if len(leaf_markers) > 0:
        markersJS = """
  var markers = [
{}
  ];
  var markersGroup = L.featureGroup(markers);
  map.addLayer(markersGroup);
  map.fitBounds(markersGroup.getBounds().pad(0.05));
""".format(",\n".join(leaf_markers))
    return markersJS


################################################################################

def __update_geo_plot_params_from_form(byc):
    p_p = byc["geoloc_definitions"].get("plot_params", {})
    p_p.update({"inputfile": BYC_PARS.get("inputfile", "")})

    bps = {}
    plot_pars = BYC_PARS.get("plot_pars", {})
    for ppv in re.split(r'::|&', plot_pars):
        pp_pv = ppv.split('=')
        if len(pp_pv) == 2:
            pp, pv = pp_pv
            pp = decamelize(pp)
            bps.update({pp: pv})

    for p_p_k, p_p_v in p_p.items():
        if p_p_k in bps:
            p_p.update({p_p_k: bps.get(p_p_k, p_p_v)})
    return p_p


################################################################################

def __marker_max_from_geo_locations(geolocs):
    m_max_count = 1
    for g_l in geolocs:
        c = float( g_l["properties"].get("marker_count", 1) )
        if c > m_max_count:
            m_max_count = c
    return m_max_count


################################################################################

def __map_marker_from_geo_location(byc, geoloc, p_p, m_max_count):
    p = geoloc.get("properties", {})
    g = geoloc.get("geometry", {})
    marker = p_p.get("marker_type", "circle")
    m_max_r = p_p.get("marker_max_r", 1000)
    m_f = int(int(m_max_r) / math.sqrt(4 * m_max_count / math.pi))

    label = p.get("label", None)
    if label is None:
        label = p.get("city", "NA")
        country = p.get("country", None)
        if country:
            label = f'{label}, {country}'

    items = p.get("items", [])
    items = [x for x in items if x is not None]
    if len(items) > 0:
        label += "<hr/>{}".format("<br/>".join(items))
    else:
        label += f'<hr/>latitude: {g["coordinates"][1]}, longitude: {g["coordinates"][0]}'

    count = float(p.get("marker_count", 1))
    size = count * m_f * float(p_p.get("marker_scale", 2))
    marker_icon = p.get("marker_icon", "")

    if ".png" in marker_icon or ".jpg" in marker_icon:
        marker = "marker"
    if "circle" in marker:
        map_marker = """
L.{}([{}, {}], {{
    ...circleOptions,
    ...{{radius: {}, count: {}}}
}}).bindPopup("{}", {{maxHeight: 200}})
    """.format(
        marker,
        g["coordinates"][1],
        g["coordinates"][0],
        size,
        count,
        label
    )

    else:
        map_marker = """
L.{}([{}, {}], {{
    ...{{count: {}}}
}}).bindPopup("{}", {{maxHeight: 200}})
    """.format(
        marker,
        g["coordinates"][1],
        g["coordinates"][0],
        count,
        label
    )

    return map_marker
