"""
Comprehensive mapping of English Local Planning Authorities.

Each entry contains:
- name: Simplified council name (without "Council", "Borough Council", etc.)
- organisation_entity: The entity code from the Planning Data API
- region: The English region
- portal_type: "idox", "civica", "nec", or "api" (Planning Data API fallback)
- portal_url: URL for councils with a portal scraper (omitted otherwise)

Source: Planning Data API (https://www.planning.data.gov.uk/)
"""

ENGLISH_LPA_MAPPING = [
    # -------------------------------------------------------------------------
    # A
    # -------------------------------------------------------------------------
    {
        "name": "Adur",
        "organisation_entity": "26",
        "region": "South East",
        "portal_type": "api",
    },
    {
        "name": "Allerdale",
        "organisation_entity": "27",
        "region": "North West",
        "portal_type": "api",
    },
    {
        "name": "Amber Valley",
        "organisation_entity": "28",
        "region": "East Midlands",
        "portal_type": "api",
    },
    {
        "name": "Arun",
        "organisation_entity": "29",
        "region": "South East",
        "portal_type": "api",
    },
    {
        "name": "Ashford",
        "organisation_entity": "30",
        "region": "South East",
        "portal_type": "api",
    },
    {
        "name": "Ashfield",
        "organisation_entity": "31",
        "region": "East Midlands",
        "portal_type": "api",
    },
    # -------------------------------------------------------------------------
    # B
    # -------------------------------------------------------------------------
    {
        "name": "Babergh",
        "organisation_entity": "33",
        "region": "East of England",
        "portal_type": "api",
    },
    {
        "name": "Bassetlaw",
        "organisation_entity": "34",
        "region": "East Midlands",
        "portal_type": "api",
    },
    {
        "name": "Basildon",
        "organisation_entity": "35",
        "region": "East of England",
        "portal_type": "api",
    },
    {
        "name": "Basingstoke and Deane",
        "organisation_entity": "36",
        "region": "South East",
        "portal_type": "api",
    },
    {
        "name": "Barrow-in-Furness",
        "organisation_entity": "37",
        "region": "North West",
        "portal_type": "api",
    },
    {
        "name": "Bath and North East Somerset",
        "organisation_entity": "38",
        "region": "West of England",
        "portal_type": "api",
    },
    {
        "name": "Blackburn with Darwen",
        "organisation_entity": "39",
        "region": "North West",
        "portal_type": "api",
    },
    {
        "name": "Bedford",
        "organisation_entity": "40",
        "region": "East of England",
        "portal_type": "api",
    },
    {
        "name": "Barking and Dagenham",
        "organisation_entity": "41",
        "region": "London",
        "portal_type": "api",
    },
    {
        "name": "Brent",
        "organisation_entity": "42",
        "region": "London",
        "portal_type": "api",
    },
    {
        "name": "Bexley",
        "organisation_entity": "43",
        "region": "London",
        "portal_type": "idox",
        "portal_url": "https://pa.bexley.gov.uk/online-applications",
    },
    {
        "name": "Birmingham",
        "organisation_entity": "44",
        "region": "West Midlands",
        "portal_type": "nec",
        "portal_url": "https://eplanning.birmingham.gov.uk",
    },
    {
        "name": "Blaby",
        "organisation_entity": "46",
        "region": "East Midlands",
        "portal_type": "api",
    },
    {
        "name": "Bournemouth",
        "organisation_entity": "47",
        "region": "South West",
        "portal_type": "api",
    },
    {
        "name": "Barnet",
        "organisation_entity": "48",
        "region": "London",
        "portal_type": "idox",
        "portal_url": "https://publicaccess.barnet.gov.uk/online-applications",
    },
    {
        "name": "Brighton and Hove",
        "organisation_entity": "49",
        "region": "South East",
        "portal_type": "idox",
        "portal_url": "https://planningapps.brighton-hove.gov.uk/online-applications",
    },
    {
        "name": "Barnsley",
        "organisation_entity": "50",
        "region": "South Yorkshire",
        "portal_type": "api",
    },
    {
        "name": "Bolton",
        "organisation_entity": "51",
        "region": "Greater Manchester",
        "portal_type": "api",
    },
    {
        "name": "Bolsover",
        "organisation_entity": "52",
        "region": "East Midlands",
        "portal_type": "api",
    },
    {
        "name": "Boston",
        "organisation_entity": "53",
        "region": "East Midlands",
        "portal_type": "api",
    },
    {
        "name": "Bournemouth, Christchurch and Poole",
        "organisation_entity": "54",
        "region": "South West",
        "portal_type": "idox",
        "portal_url": "https://planning.bournemouth.gov.uk/online-applications",
    },
    {
        "name": "Blackpool",
        "organisation_entity": "55",
        "region": "North West",
        "portal_type": "api",
    },
    {
        "name": "Braintree",
        "organisation_entity": "56",
        "region": "East of England",
        "portal_type": "api",
    },
    {
        "name": "Bracknell Forest",
        "organisation_entity": "57",
        "region": "South East",
        "portal_type": "api",
    },
    {
        "name": "Bradford",
        "organisation_entity": "58",
        "region": "West Yorkshire",
        "portal_type": "idox",
        "portal_url": "https://planning.bradford.gov.uk/online-applications",
    },
    {
        "name": "Breckland",
        "organisation_entity": "59",
        "region": "East of England",
        "portal_type": "api",
    },
    {
        "name": "Bromsgrove",
        "organisation_entity": "60",
        "region": "West Midlands",
        "portal_type": "api",
    },
    {
        "name": "Broadland",
        "organisation_entity": "61",
        "region": "East of England",
        "portal_type": "api",
    },
    {
        "name": "Broxtowe",
        "organisation_entity": "62",
        "region": "East Midlands",
        "portal_type": "api",
    },
    {
        "name": "Brentwood",
        "organisation_entity": "63",
        "region": "East of England",
        "portal_type": "api",
    },
    {
        "name": "Broxbourne",
        "organisation_entity": "64",
        "region": "East of England",
        "portal_type": "api",
    },
    {
        "name": "Bromley",
        "organisation_entity": "65",
        "region": "London",
        "portal_type": "idox",
        "portal_url": "https://searchapplications.bromley.gov.uk/online-applications",
    },
    {
        "name": "Bristol",
        "organisation_entity": "66",
        "region": "West of England",
        "portal_type": "idox",
        "portal_url": "https://pa.bristol.gov.uk/online-applications",
    },
    {
        "name": "Buckinghamshire",
        "organisation_entity": "67",
        "region": "South East",
        "portal_type": "api",
    },
    {
        "name": "Burnley",
        "organisation_entity": "68",
        "region": "North West",
        "portal_type": "api",
    },
    {
        "name": "Bury",
        "organisation_entity": "69",
        "region": "Greater Manchester",
        "portal_type": "api",
    },
    # -------------------------------------------------------------------------
    # C
    # -------------------------------------------------------------------------
    {
        "name": "Cambridge",
        "organisation_entity": "70",
        "region": "East of England",
        "portal_type": "idox",
        "portal_url": "https://applications.greatercambridgeplanning.org/online-applications",
    },
    {
        "name": "Cannock Chase",
        "organisation_entity": "72",
        "region": "West Midlands",
        "portal_type": "api",
    },
    {
        "name": "Carlisle",
        "organisation_entity": "73",
        "region": "North West",
        "portal_type": "api",
    },
    {
        "name": "Castle Point",
        "organisation_entity": "74",
        "region": "East of England",
        "portal_type": "api",
    },
    {
        "name": "Canterbury",
        "organisation_entity": "75",
        "region": "South East",
        "portal_type": "api",
    },
    {
        "name": "Central Bedfordshire",
        "organisation_entity": "76",
        "region": "East of England",
        "portal_type": "api",
    },
    {
        "name": "Charnwood",
        "organisation_entity": "77",
        "region": "East Midlands",
        "portal_type": "api",
    },
    {
        "name": "Cheshire East",
        "organisation_entity": "79",
        "region": "North West",
        "portal_type": "api",
    },
    {
        "name": "Chichester",
        "organisation_entity": "80",
        "region": "South East",
        "portal_type": "api",
    },
    {
        "name": "Chelmsford",
        "organisation_entity": "81",
        "region": "East of England",
        "portal_type": "api",
    },
    {
        "name": "Chorley",
        "organisation_entity": "83",
        "region": "North West",
        "portal_type": "api",
    },
    {
        "name": "Cherwell",
        "organisation_entity": "84",
        "region": "South East",
        "portal_type": "api",
    },
    {
        "name": "Chesterfield",
        "organisation_entity": "85",
        "region": "East Midlands",
        "portal_type": "api",
    },
    {
        "name": "Cheltenham",
        "organisation_entity": "86",
        "region": "South West",
        "portal_type": "api",
    },
    {
        "name": "Cheshire West and Chester",
        "organisation_entity": "87",
        "region": "North West",
        "portal_type": "api",
    },
    {
        "name": "Calderdale",
        "organisation_entity": "88",
        "region": "West Yorkshire",
        "portal_type": "api",
    },
    {
        "name": "Camden",
        "organisation_entity": "90",
        "region": "London",
        "portal_type": "idox",
        "portal_url": "https://publicaccess.camden.gov.uk/online-applications",
    },
    {
        "name": "Colchester",
        "organisation_entity": "91",
        "region": "East of England",
        "portal_type": "api",
    },
    {
        "name": "Cornwall",
        "organisation_entity": "92",
        "region": "South West",
        "portal_type": "api",
    },
    {
        "name": "Copeland",
        "organisation_entity": "93",
        "region": "North West",
        "portal_type": "api",
    },
    {
        "name": "Cotswold",
        "organisation_entity": "95",
        "region": "South West",
        "portal_type": "api",
    },
    {
        "name": "Coventry",
        "organisation_entity": "96",
        "region": "West Midlands",
        "portal_type": "api",
    },
    {
        "name": "Craven",
        "organisation_entity": "98",
        "region": "Yorkshire and the Humber",
        "portal_type": "api",
    },
    {
        "name": "Crawley",
        "organisation_entity": "99",
        "region": "South East",
        "portal_type": "api",
    },
    {
        "name": "Croydon",
        "organisation_entity": "100",
        "region": "London",
        "portal_type": "idox",
        "portal_url": "https://publicaccess3.croydon.gov.uk/online-applications",
    },
    # -------------------------------------------------------------------------
    # D
    # -------------------------------------------------------------------------
    {
        "name": "Dacorum",
        "organisation_entity": "101",
        "region": "East of England",
        "portal_type": "api",
    },
    {
        "name": "Darlington",
        "organisation_entity": "102",
        "region": "North East",
        "portal_type": "api",
    },
    {
        "name": "Dartford",
        "organisation_entity": "103",
        "region": "South East",
        "portal_type": "api",
    },
    {
        "name": "Derbyshire Dales",
        "organisation_entity": "106",
        "region": "East Midlands",
        "portal_type": "api",
    },
    {
        "name": "Derby",
        "organisation_entity": "107",
        "region": "East Midlands",
        "portal_type": "idox",
        "portal_url": "https://eplanning.derby.gov.uk/online-applications",
    },
    {
        "name": "Doncaster",
        "organisation_entity": "109",
        "region": "South Yorkshire",
        "portal_type": "api",
    },
    {
        "name": "Dover",
        "organisation_entity": "111",
        "region": "South East",
        "portal_type": "api",
    },
    {
        "name": "Dorset",
        "organisation_entity": "112",
        "region": "South West",
        "portal_type": "api",
    },
    {
        "name": "Dudley",
        "organisation_entity": "113",
        "region": "West Midlands",
        "portal_type": "api",
    },
    {
        "name": "Durham",
        "organisation_entity": "114",
        "region": "North East",
        "portal_type": "api",
    },
    # -------------------------------------------------------------------------
    # E
    # -------------------------------------------------------------------------
    {
        "name": "Ealing",
        "organisation_entity": "115",
        "region": "London",
        "portal_type": "idox",
        "portal_url": "https://pam.ealing.gov.uk/online-applications",
    },
    {
        "name": "Eastbourne",
        "organisation_entity": "116",
        "region": "South East",
        "portal_type": "api",
    },
    {
        "name": "Eastleigh",
        "organisation_entity": "117",
        "region": "South East",
        "portal_type": "api",
    },
    {
        "name": "East Cambridgeshire",
        "organisation_entity": "118",
        "region": "East of England",
        "portal_type": "api",
    },
    {
        "name": "East Devon",
        "organisation_entity": "119",
        "region": "South West",
        "portal_type": "api",
    },
    {
        "name": "East Hampshire",
        "organisation_entity": "122",
        "region": "South East",
        "portal_type": "api",
    },
    {
        "name": "East Hertfordshire",
        "organisation_entity": "123",
        "region": "East of England",
        "portal_type": "api",
    },
    {
        "name": "East Lindsey",
        "organisation_entity": "124",
        "region": "East Midlands",
        "portal_type": "api",
    },
    {
        "name": "Elmbridge",
        "organisation_entity": "125",
        "region": "South East",
        "portal_type": "api",
    },
    {
        "name": "Enfield",
        "organisation_entity": "126",
        "region": "London",
        "portal_type": "idox",
        "portal_url": "https://planningandbuildingcontrol.enfield.gov.uk/online-applications",
    },
    {
        "name": "Epping Forest",
        "organisation_entity": "128",
        "region": "East of England",
        "portal_type": "api",
    },
    {
        "name": "Epsom and Ewell",
        "organisation_entity": "129",
        "region": "South East",
        "portal_type": "api",
    },
    {
        "name": "Erewash",
        "organisation_entity": "130",
        "region": "East Midlands",
        "portal_type": "api",
    },
    {
        "name": "East Riding of Yorkshire",
        "organisation_entity": "131",
        "region": "Yorkshire and the Humber",
        "portal_type": "api",
    },
    {
        "name": "East Suffolk",
        "organisation_entity": "132",
        "region": "East of England",
        "portal_type": "api",
    },
    {
        "name": "East Staffordshire",
        "organisation_entity": "134",
        "region": "West Midlands",
        "portal_type": "api",
    },
    {
        "name": "Exeter",
        "organisation_entity": "136",
        "region": "South West",
        "portal_type": "idox",
        "portal_url": "https://publicaccess.exeter.gov.uk/online-applications",
    },
    # -------------------------------------------------------------------------
    # F
    # -------------------------------------------------------------------------
    {
        "name": "Fareham",
        "organisation_entity": "137",
        "region": "South East",
        "portal_type": "api",
    },
    {
        "name": "Fenland",
        "organisation_entity": "138",
        "region": "East of England",
        "portal_type": "api",
    },
    {
        "name": "Forest of Dean",
        "organisation_entity": "139",
        "region": "South West",
        "portal_type": "api",
    },
    {
        "name": "Folkestone and Hythe",
        "organisation_entity": "293",
        "region": "South East",
        "portal_type": "api",
    },
    {
        "name": "Fylde",
        "organisation_entity": "141",
        "region": "North West",
        "portal_type": "api",
    },
    # -------------------------------------------------------------------------
    # G
    # -------------------------------------------------------------------------
    {
        "name": "Gateshead",
        "organisation_entity": "142",
        "region": "Tyne and Wear",
        "portal_type": "api",
    },
    {
        "name": "Gedling",
        "organisation_entity": "143",
        "region": "East Midlands",
        "portal_type": "api",
    },
    {
        "name": "Gloucester",
        "organisation_entity": "145",
        "region": "South West",
        "portal_type": "idox",
        "portal_url": "https://publicaccess.gloucester.gov.uk/online-applications",
    },
    {
        "name": "Gosport",
        "organisation_entity": "148",
        "region": "South East",
        "portal_type": "api",
    },
    {
        "name": "Gravesham",
        "organisation_entity": "149",
        "region": "South East",
        "portal_type": "api",
    },
    {
        "name": "Greenwich",
        "organisation_entity": "150",
        "region": "London",
        "portal_type": "idox",
        "portal_url": "https://planning.royalgreenwich.gov.uk/online-applications",
    },
    {
        "name": "Guildford",
        "organisation_entity": "151",
        "region": "South East",
        "portal_type": "api",
    },
    {
        "name": "Great Yarmouth",
        "organisation_entity": "152",
        "region": "East of England",
        "portal_type": "api",
    },
    # -------------------------------------------------------------------------
    # H
    # -------------------------------------------------------------------------
    {
        "name": "Havant",
        "organisation_entity": "153",
        "region": "South East",
        "portal_type": "api",
    },
    {
        "name": "Hambleton",
        "organisation_entity": "154",
        "region": "Yorkshire and the Humber",
        "portal_type": "api",
    },
    {
        "name": "Harrogate",
        "organisation_entity": "155",
        "region": "Yorkshire and the Humber",
        "portal_type": "api",
    },
    {
        "name": "Halton",
        "organisation_entity": "156",
        "region": "Merseyside",
        "portal_type": "api",
    },
    {
        "name": "Harborough",
        "organisation_entity": "158",
        "region": "East Midlands",
        "portal_type": "api",
    },
    {
        "name": "Harlow",
        "organisation_entity": "159",
        "region": "East of England",
        "portal_type": "api",
    },
    {
        "name": "Hastings",
        "organisation_entity": "160",
        "region": "South East",
        "portal_type": "api",
    },
    {
        "name": "Hart",
        "organisation_entity": "161",
        "region": "South East",
        "portal_type": "api",
    },
    {
        "name": "Havering",
        "organisation_entity": "162",
        "region": "London",
        "portal_type": "civica",
        "portal_url": "https://development.havering.gov.uk",
    },
    {
        "name": "Hackney",
        "organisation_entity": "163",
        "region": "London",
        "portal_type": "idox",
        "portal_url": "https://publicaccess.hackney.gov.uk/online-applications",
    },
    {
        "name": "Herefordshire",
        "organisation_entity": "164",
        "region": "West Midlands",
        "portal_type": "api",
    },
    {
        "name": "Hertsmere",
        "organisation_entity": "165",
        "region": "East of England",
        "portal_type": "api",
    },
    {
        "name": "High Peak",
        "organisation_entity": "166",
        "region": "East Midlands",
        "portal_type": "api",
    },
    {
        "name": "Hillingdon",
        "organisation_entity": "167",
        "region": "London",
        "portal_type": "civica",
        "portal_url": "https://planning.hillingdon.gov.uk",
    },
    {
        "name": "Hinckley and Bosworth",
        "organisation_entity": "168",
        "region": "East Midlands",
        "portal_type": "api",
    },
    {
        "name": "Hammersmith and Fulham",
        "organisation_entity": "169",
        "region": "London",
        "portal_type": "api",
    },
    {
        "name": "Hounslow",
        "organisation_entity": "170",
        "region": "London",
        "portal_type": "civica",
        "portal_url": "https://planning.hounslow.gov.uk",
    },
    {
        "name": "Horsham",
        "organisation_entity": "171",
        "region": "South East",
        "portal_type": "api",
    },
    {
        "name": "Hartlepool",
        "organisation_entity": "172",
        "region": "North East",
        "portal_type": "api",
    },
    {
        "name": "Harrow",
        "organisation_entity": "174",
        "region": "London",
        "portal_type": "idox",
        "portal_url": "https://planningsearch.harrow.gov.uk/online-applications",
    },
    {
        "name": "Haringey",
        "organisation_entity": "175",
        "region": "London",
        "portal_type": "api",
    },
    {
        "name": "Huntingdonshire",
        "organisation_entity": "176",
        "region": "East of England",
        "portal_type": "api",
    },
    {
        "name": "Hyndburn",
        "organisation_entity": "177",
        "region": "North West",
        "portal_type": "api",
    },
    {
        "name": "Hull",
        "organisation_entity": "185",
        "region": "Yorkshire and the Humber",
        "portal_type": "api",
    },
    # -------------------------------------------------------------------------
    # I
    # -------------------------------------------------------------------------
    {
        "name": "Isle of Wight",
        "organisation_entity": "179",
        "region": "South East",
        "portal_type": "api",
    },
    {
        "name": "Ipswich",
        "organisation_entity": "180",
        "region": "East of England",
        "portal_type": "api",
    },
    {
        "name": "Islington",
        "organisation_entity": "181",
        "region": "London",
        "portal_type": "api",
    },
    # -------------------------------------------------------------------------
    # K
    # -------------------------------------------------------------------------
    {
        "name": "Kensington and Chelsea",
        "organisation_entity": "182",
        "region": "London",
        "portal_type": "api",
    },
    {
        "name": "King's Lynn and West Norfolk",
        "organisation_entity": "186",
        "region": "East of England",
        "portal_type": "api",
    },
    {
        "name": "Kirklees",
        "organisation_entity": "187",
        "region": "West Yorkshire",
        "portal_type": "api",
    },
    {
        "name": "Kingston upon Thames",
        "organisation_entity": "188",
        "region": "London",
        "portal_type": "idox",
        "portal_url": "https://publicaccess.kingston.gov.uk/online-applications",
    },
    {
        "name": "Knowsley",
        "organisation_entity": "189",
        "region": "Merseyside",
        "portal_type": "api",
    },
    # -------------------------------------------------------------------------
    # L
    # -------------------------------------------------------------------------
    {
        "name": "Lancaster",
        "organisation_entity": "190",
        "region": "North West",
        "portal_type": "api",
    },
    {
        "name": "Lambeth",
        "organisation_entity": "192",
        "region": "London",
        "portal_type": "idox",
        "portal_url": "https://planning.lambeth.gov.uk/online-applications",
    },
    {
        "name": "Leicester",
        "organisation_entity": "193",
        "region": "East Midlands",
        "portal_type": "idox",
        "portal_url": "https://planning.leicester.gov.uk/online-applications",
    },
    {
        "name": "Leeds",
        "organisation_entity": "195",
        "region": "West Yorkshire",
        "portal_type": "idox",
        "portal_url": "https://publicaccess.leeds.gov.uk/online-applications",
    },
    {
        "name": "Lewes",
        "organisation_entity": "197",
        "region": "South East",
        "portal_type": "api",
    },
    {
        "name": "Lewisham",
        "organisation_entity": "198",
        "region": "London",
        "portal_type": "idox",
        "portal_url": "https://planning.lewisham.gov.uk/online-applications",
    },
    {
        "name": "Lincoln",
        "organisation_entity": "199",
        "region": "East Midlands",
        "portal_type": "api",
    },
    {
        "name": "Lichfield",
        "organisation_entity": "200",
        "region": "West Midlands",
        "portal_type": "api",
    },
    {
        "name": "Liverpool",
        "organisation_entity": "202",
        "region": "Merseyside",
        "portal_type": "nec",
        "portal_url": "http://northgate.liverpool.gov.uk",
    },
    {
        "name": "City of London",
        "organisation_entity": "203",
        "region": "London",
        "portal_type": "api",
    },
    {
        "name": "Luton",
        "organisation_entity": "204",
        "region": "East of England",
        "portal_type": "idox",
        "portal_url": "https://planning.luton.gov.uk/online-applications",
    },
    # -------------------------------------------------------------------------
    # M
    # -------------------------------------------------------------------------
    {
        "name": "Maidstone",
        "organisation_entity": "205",
        "region": "South East",
        "portal_type": "api",
    },
    {
        "name": "Maldon",
        "organisation_entity": "206",
        "region": "East of England",
        "portal_type": "api",
    },
    {
        "name": "Manchester",
        "organisation_entity": "207",
        "region": "Greater Manchester",
        "portal_type": "idox",
        "portal_url": "https://pa.manchester.gov.uk/online-applications",
    },
    {
        "name": "Mansfield",
        "organisation_entity": "208",
        "region": "East Midlands",
        "portal_type": "api",
    },
    {
        "name": "Malvern Hills",
        "organisation_entity": "209",
        "region": "West Midlands",
        "portal_type": "api",
    },
    {
        "name": "Middlesbrough",
        "organisation_entity": "210",
        "region": "North East",
        "portal_type": "api",
    },
    {
        "name": "Mid Devon",
        "organisation_entity": "211",
        "region": "South West",
        "portal_type": "api",
    },
    {
        "name": "Medway",
        "organisation_entity": "212",
        "region": "South East",
        "portal_type": "api",
    },
    {
        "name": "Melton",
        "organisation_entity": "213",
        "region": "East Midlands",
        "portal_type": "api",
    },
    {
        "name": "Mendip",
        "organisation_entity": "214",
        "region": "South West",
        "portal_type": "api",
    },
    {
        "name": "Milton Keynes",
        "organisation_entity": "215",
        "region": "South East",
        "portal_type": "api",
    },
    {
        "name": "Mole Valley",
        "organisation_entity": "216",
        "region": "South East",
        "portal_type": "api",
    },
    {
        "name": "Merton",
        "organisation_entity": "217",
        "region": "London",
        "portal_type": "nec",
        "portal_url": "https://planning.merton.gov.uk",
    },
    {
        "name": "Mid Sussex",
        "organisation_entity": "218",
        "region": "South East",
        "portal_type": "api",
    },
    {
        "name": "Mid Suffolk",
        "organisation_entity": "219",
        "region": "East of England",
        "portal_type": "api",
    },
    # -------------------------------------------------------------------------
    # N
    # -------------------------------------------------------------------------
    {
        "name": "Northumberland",
        "organisation_entity": "220",
        "region": "North East",
        "portal_type": "api",
    },
    {
        "name": "North Devon",
        "organisation_entity": "221",
        "region": "South West",
        "portal_type": "api",
    },
    {
        "name": "Newark and Sherwood",
        "organisation_entity": "223",
        "region": "East Midlands",
        "portal_type": "api",
    },
    {
        "name": "Newcastle-under-Lyme",
        "organisation_entity": "224",
        "region": "West Midlands",
        "portal_type": "api",
    },
    {
        "name": "North East Derbyshire",
        "organisation_entity": "226",
        "region": "East Midlands",
        "portal_type": "api",
    },
    {
        "name": "North East Lincolnshire",
        "organisation_entity": "227",
        "region": "Yorkshire and the Humber",
        "portal_type": "api",
    },
    {
        "name": "Newcastle",
        "organisation_entity": "228",
        "region": "Tyne and Wear",
        "portal_type": "api",
    },
    {
        "name": "New Forest",
        "organisation_entity": "229",
        "region": "South East",
        "portal_type": "api",
    },
    {
        "name": "Nottingham",
        "organisation_entity": "231",
        "region": "East Midlands",
        "portal_type": "idox",
        "portal_url": "https://publicaccess.nottinghamcity.gov.uk/online-applications",
    },
    {
        "name": "North Hertfordshire",
        "organisation_entity": "232",
        "region": "East of England",
        "portal_type": "api",
    },
    {
        "name": "North Kesteven",
        "organisation_entity": "233",
        "region": "East Midlands",
        "portal_type": "api",
    },
    {
        "name": "North Lincolnshire",
        "organisation_entity": "234",
        "region": "Yorkshire and the Humber",
        "portal_type": "api",
    },
    {
        "name": "North Norfolk",
        "organisation_entity": "235",
        "region": "East of England",
        "portal_type": "api",
    },
    {
        "name": "Norwich",
        "organisation_entity": "237",
        "region": "East of England",
        "portal_type": "api",
    },
    {
        "name": "North Somerset",
        "organisation_entity": "238",
        "region": "West of England",
        "portal_type": "api",
    },
    {
        "name": "North Tyneside",
        "organisation_entity": "242",
        "region": "Tyne and Wear",
        "portal_type": "api",
    },
    {
        "name": "Nuneaton and Bedworth",
        "organisation_entity": "243",
        "region": "West Midlands",
        "portal_type": "api",
    },
    {
        "name": "North Warwickshire",
        "organisation_entity": "244",
        "region": "West Midlands",
        "portal_type": "api",
    },
    {
        "name": "North West Leicestershire",
        "organisation_entity": "245",
        "region": "East Midlands",
        "portal_type": "api",
    },
    {
        "name": "Newham",
        "organisation_entity": "246",
        "region": "London",
        "portal_type": "api",
    },
    {
        "name": "North Northamptonshire",
        "organisation_entity": "501908",
        "region": "East Midlands",
        "portal_type": "api",
    },
    {
        "name": "North Yorkshire",
        "organisation_entity": "10004",
        "region": "Yorkshire and the Humber",
        "portal_type": "api",
    },
    # -------------------------------------------------------------------------
    # O
    # -------------------------------------------------------------------------
    {
        "name": "Oadby and Wigston",
        "organisation_entity": "248",
        "region": "East Midlands",
        "portal_type": "api",
    },
    {
        "name": "Oldham",
        "organisation_entity": "249",
        "region": "Greater Manchester",
        "portal_type": "api",
    },
    {
        "name": "Oxford",
        "organisation_entity": "251",
        "region": "South East",
        "portal_type": "idox",
        "portal_url": "https://public.oxford.gov.uk/online-applications",
    },
    # -------------------------------------------------------------------------
    # P
    # -------------------------------------------------------------------------
    {
        "name": "Pendle",
        "organisation_entity": "252",
        "region": "North West",
        "portal_type": "api",
    },
    {
        "name": "Plymouth",
        "organisation_entity": "253",
        "region": "South West",
        "portal_type": "idox",
        "portal_url": "https://planning.plymouth.gov.uk/online-applications",
    },
    {
        "name": "Portsmouth",
        "organisation_entity": "255",
        "region": "South East",
        "portal_type": "idox",
        "portal_url": "https://publicaccess.portsmouth.gov.uk/online-applications",
    },
    {
        "name": "Preston",
        "organisation_entity": "256",
        "region": "North West",
        "portal_type": "api",
    },
    {
        "name": "Peterborough",
        "organisation_entity": "257",
        "region": "East of England",
        "portal_type": "idox",
        "portal_url": "https://planpa.peterborough.gov.uk/online-applications",
    },
    # -------------------------------------------------------------------------
    # R
    # -------------------------------------------------------------------------
    {
        "name": "Redcar and Cleveland",
        "organisation_entity": "259",
        "region": "North East",
        "portal_type": "api",
    },
    {
        "name": "Rochdale",
        "organisation_entity": "260",
        "region": "Greater Manchester",
        "portal_type": "api",
    },
    {
        "name": "Redbridge",
        "organisation_entity": "261",
        "region": "London",
        "portal_type": "api",
    },
    {
        "name": "Reading",
        "organisation_entity": "262",
        "region": "South East",
        "portal_type": "nec",
        "portal_url": "https://planning.reading.gov.uk/fastweb_PL",
    },
    {
        "name": "Redditch",
        "organisation_entity": "263",
        "region": "West Midlands",
        "portal_type": "api",
    },
    {
        "name": "Reigate and Banstead",
        "organisation_entity": "264",
        "region": "South East",
        "portal_type": "api",
    },
    {
        "name": "Ribble Valley",
        "organisation_entity": "265",
        "region": "North West",
        "portal_type": "api",
    },
    {
        "name": "Richmond upon Thames",
        "organisation_entity": "266",
        "region": "London",
        "portal_type": "api",
    },
    {
        "name": "Richmondshire",
        "organisation_entity": "267",
        "region": "Yorkshire and the Humber",
        "portal_type": "api",
    },
    {
        "name": "Rochford",
        "organisation_entity": "268",
        "region": "East of England",
        "portal_type": "api",
    },
    {
        "name": "Rother",
        "organisation_entity": "269",
        "region": "South East",
        "portal_type": "api",
    },
    {
        "name": "Rossendale",
        "organisation_entity": "270",
        "region": "North West",
        "portal_type": "api",
    },
    {
        "name": "Rotherham",
        "organisation_entity": "271",
        "region": "South Yorkshire",
        "portal_type": "api",
    },
    {
        "name": "Rugby",
        "organisation_entity": "272",
        "region": "West Midlands",
        "portal_type": "api",
    },
    {
        "name": "Rushmoor",
        "organisation_entity": "273",
        "region": "South East",
        "portal_type": "api",
    },
    {
        "name": "Runnymede",
        "organisation_entity": "274",
        "region": "South East",
        "portal_type": "api",
    },
    {
        "name": "Rushcliffe",
        "organisation_entity": "275",
        "region": "East Midlands",
        "portal_type": "api",
    },
    {
        "name": "Rutland",
        "organisation_entity": "276",
        "region": "East Midlands",
        "portal_type": "api",
    },
    # -------------------------------------------------------------------------
    # S
    # -------------------------------------------------------------------------
    {
        "name": "St Albans",
        "organisation_entity": "278",
        "region": "East of England",
        "portal_type": "api",
    },
    {
        "name": "Sandwell",
        "organisation_entity": "279",
        "region": "West Midlands",
        "portal_type": "api",
    },
    {
        "name": "South Cambridgeshire",
        "organisation_entity": "281",
        "region": "East of England",
        "portal_type": "api",
    },
    {
        "name": "South Derbyshire",
        "organisation_entity": "284",
        "region": "East Midlands",
        "portal_type": "api",
    },
    {
        "name": "Sedgemoor",
        "organisation_entity": "286",
        "region": "South West",
        "portal_type": "api",
    },
    {
        "name": "Sevenoaks",
        "organisation_entity": "288",
        "region": "South East",
        "portal_type": "api",
    },
    {
        "name": "Sefton",
        "organisation_entity": "290",
        "region": "Merseyside",
        "portal_type": "api",
    },
    {
        "name": "South Gloucestershire",
        "organisation_entity": "291",
        "region": "West of England",
        "portal_type": "api",
    },
    {
        "name": "South Hams",
        "organisation_entity": "292",
        "region": "South West",
        "portal_type": "api",
    },
    {
        "name": "Sheffield",
        "organisation_entity": "294",
        "region": "South Yorkshire",
        "portal_type": "idox",
        "portal_url": "https://planningapps.sheffield.gov.uk/online-applications",
    },
    {
        "name": "St Helens",
        "organisation_entity": "295",
        "region": "Merseyside",
        "portal_type": "api",
    },
    {
        "name": "South Holland",
        "organisation_entity": "296",
        "region": "East Midlands",
        "portal_type": "api",
    },
    {
        "name": "Shropshire",
        "organisation_entity": "297",
        "region": "West Midlands",
        "portal_type": "api",
    },
    {
        "name": "South Kesteven",
        "organisation_entity": "298",
        "region": "East Midlands",
        "portal_type": "api",
    },
    {
        "name": "Stockport",
        "organisation_entity": "299",
        "region": "Greater Manchester",
        "portal_type": "api",
    },
    {
        "name": "Salford",
        "organisation_entity": "301",
        "region": "Greater Manchester",
        "portal_type": "api",
    },
    {
        "name": "Slough",
        "organisation_entity": "302",
        "region": "South East",
        "portal_type": "api",
    },
    {
        "name": "Sunderland",
        "organisation_entity": "303",
        "region": "Tyne and Wear",
        "portal_type": "idox",
        "portal_url": "https://online-applications.sunderland.gov.uk/online-applications",
    },
    {
        "name": "South Norfolk",
        "organisation_entity": "304",
        "region": "East of England",
        "portal_type": "api",
    },
    {
        "name": "Solihull",
        "organisation_entity": "306",
        "region": "West Midlands",
        "portal_type": "api",
    },
    {
        "name": "Southend-on-Sea",
        "organisation_entity": "308",
        "region": "East of England",
        "portal_type": "api",
    },
    {
        "name": "South Oxfordshire",
        "organisation_entity": "309",
        "region": "South East",
        "portal_type": "api",
    },
    {
        "name": "Spelthorne",
        "organisation_entity": "310",
        "region": "South East",
        "portal_type": "api",
    },
    {
        "name": "South Ribble",
        "organisation_entity": "311",
        "region": "North West",
        "portal_type": "api",
    },
    {
        "name": "South Somerset",
        "organisation_entity": "313",
        "region": "South West",
        "portal_type": "api",
    },
    {
        "name": "South Staffordshire",
        "organisation_entity": "314",
        "region": "West Midlands",
        "portal_type": "api",
    },
    {
        "name": "Stafford",
        "organisation_entity": "315",
        "region": "West Midlands",
        "portal_type": "api",
    },
    {
        "name": "Stoke-on-Trent",
        "organisation_entity": "316",
        "region": "West Midlands",
        "portal_type": "api",
    },
    {
        "name": "Staffordshire Moorlands",
        "organisation_entity": "317",
        "region": "West Midlands",
        "portal_type": "api",
    },
    {
        "name": "Southampton",
        "organisation_entity": "318",
        "region": "South East",
        "portal_type": "idox",
        "portal_url": "https://planningpublicaccess.southampton.gov.uk/online-applications",
    },
    {
        "name": "Sutton",
        "organisation_entity": "319",
        "region": "London",
        "portal_type": "idox",
        "portal_url": "https://planningregister.sutton.gov.uk/online-applications",
    },
    {
        "name": "Stroud",
        "organisation_entity": "320",
        "region": "South West",
        "portal_type": "api",
    },
    {
        "name": "Stratford-on-Avon",
        "organisation_entity": "321",
        "region": "West Midlands",
        "portal_type": "api",
    },
    {
        "name": "Stockton-on-Tees",
        "organisation_entity": "323",
        "region": "North East",
        "portal_type": "api",
    },
    {
        "name": "Stevenage",
        "organisation_entity": "324",
        "region": "East of England",
        "portal_type": "api",
    },
    {
        "name": "South Tyneside",
        "organisation_entity": "325",
        "region": "Tyne and Wear",
        "portal_type": "api",
    },
    {
        "name": "Surrey Heath",
        "organisation_entity": "327",
        "region": "South East",
        "portal_type": "api",
    },
    {
        "name": "Swindon",
        "organisation_entity": "328",
        "region": "South West",
        "portal_type": "api",
    },
    {
        "name": "Southwark",
        "organisation_entity": "329",
        "region": "London",
        "portal_type": "idox",
        "portal_url": "https://planning.southwark.gov.uk/online-applications",
    },
    {
        "name": "Swale",
        "organisation_entity": "330",
        "region": "South East",
        "portal_type": "api",
    },
    {
        "name": "Somerset West and Taunton",
        "organisation_entity": "331",
        "region": "South West",
        "portal_type": "api",
    },
    {
        "name": "Somerset",
        "organisation_entity": "10003",
        "region": "South West",
        "portal_type": "api",
    },
    # -------------------------------------------------------------------------
    # T
    # -------------------------------------------------------------------------
    {
        "name": "Tameside",
        "organisation_entity": "332",
        "region": "Greater Manchester",
        "portal_type": "api",
    },
    {
        "name": "Tandridge",
        "organisation_entity": "333",
        "region": "South East",
        "portal_type": "api",
    },
    {
        "name": "Tamworth",
        "organisation_entity": "335",
        "region": "West Midlands",
        "portal_type": "api",
    },
    {
        "name": "Teignbridge",
        "organisation_entity": "336",
        "region": "South West",
        "portal_type": "api",
    },
    {
        "name": "Tendring",
        "organisation_entity": "337",
        "region": "East of England",
        "portal_type": "api",
    },
    {
        "name": "Test Valley",
        "organisation_entity": "338",
        "region": "South East",
        "portal_type": "api",
    },
    {
        "name": "Tewkesbury",
        "organisation_entity": "339",
        "region": "South West",
        "portal_type": "api",
    },
    {
        "name": "Telford and Wrekin",
        "organisation_entity": "340",
        "region": "West Midlands",
        "portal_type": "api",
    },
    {
        "name": "Thanet",
        "organisation_entity": "341",
        "region": "South East",
        "portal_type": "api",
    },
    {
        "name": "Three Rivers",
        "organisation_entity": "342",
        "region": "East of England",
        "portal_type": "api",
    },
    {
        "name": "Thurrock",
        "organisation_entity": "343",
        "region": "East of England",
        "portal_type": "api",
    },
    {
        "name": "Torbay",
        "organisation_entity": "344",
        "region": "South West",
        "portal_type": "api",
    },
    {
        "name": "Tonbridge and Malling",
        "organisation_entity": "345",
        "region": "South East",
        "portal_type": "api",
    },
    {
        "name": "Torridge",
        "organisation_entity": "346",
        "region": "South West",
        "portal_type": "api",
    },
    {
        "name": "Trafford",
        "organisation_entity": "347",
        "region": "Greater Manchester",
        "portal_type": "api",
    },
    {
        "name": "Tunbridge Wells",
        "organisation_entity": "348",
        "region": "South East",
        "portal_type": "api",
    },
    {
        "name": "Tower Hamlets",
        "organisation_entity": "350",
        "region": "London",
        "portal_type": "idox",
        "portal_url": "https://development.towerhamlets.gov.uk/online-applications",
    },
    # -------------------------------------------------------------------------
    # U
    # -------------------------------------------------------------------------
    {
        "name": "Uttlesford",
        "organisation_entity": "351",
        "region": "East of England",
        "portal_type": "api",
    },
    # -------------------------------------------------------------------------
    # V
    # -------------------------------------------------------------------------
    {
        "name": "Vale of White Horse",
        "organisation_entity": "352",
        "region": "South East",
        "portal_type": "api",
    },
    # -------------------------------------------------------------------------
    # W
    # -------------------------------------------------------------------------
    {
        "name": "Waverley",
        "organisation_entity": "353",
        "region": "South East",
        "portal_type": "api",
    },
    {
        "name": "Watford",
        "organisation_entity": "355",
        "region": "East of England",
        "portal_type": "api",
    },
    {
        "name": "Warwick",
        "organisation_entity": "357",
        "region": "West Midlands",
        "portal_type": "api",
    },
    {
        "name": "West Berkshire",
        "organisation_entity": "358",
        "region": "South East",
        "portal_type": "api",
    },
    {
        "name": "West Devon",
        "organisation_entity": "359",
        "region": "South West",
        "portal_type": "api",
    },
    {
        "name": "Wealden",
        "organisation_entity": "361",
        "region": "South East",
        "portal_type": "api",
    },
    {
        "name": "Welwyn Hatfield",
        "organisation_entity": "364",
        "region": "East of England",
        "portal_type": "api",
    },
    {
        "name": "Waltham Forest",
        "organisation_entity": "366",
        "region": "London",
        "portal_type": "api",
    },
    {
        "name": "Wigan",
        "organisation_entity": "367",
        "region": "Greater Manchester",
        "portal_type": "api",
    },
    {
        "name": "Wiltshire",
        "organisation_entity": "368",
        "region": "South West",
        "portal_type": "api",
    },
    {
        "name": "Winchester",
        "organisation_entity": "369",
        "region": "South East",
        "portal_type": "api",
    },
    {
        "name": "Wakefield",
        "organisation_entity": "370",
        "region": "West Yorkshire",
        "portal_type": "api",
    },
    {
        "name": "West Lancashire",
        "organisation_entity": "371",
        "region": "North West",
        "portal_type": "api",
    },
    {
        "name": "West Lindsey",
        "organisation_entity": "372",
        "region": "East Midlands",
        "portal_type": "api",
    },
    {
        "name": "Walsall",
        "organisation_entity": "373",
        "region": "West Midlands",
        "portal_type": "idox",
        "portal_url": "https://planning.walsall.gov.uk/online-applications",
    },
    {
        "name": "Wolverhampton",
        "organisation_entity": "374",
        "region": "West Midlands",
        "portal_type": "idox",
        "portal_url": "https://planning.wolverhampton.gov.uk/online-applications",
    },
    {
        "name": "Wandsworth",
        "organisation_entity": "376",
        "region": "London",
        "portal_type": "nec",
        "portal_url": "https://planning.wandsworth.gov.uk",
    },
    {
        "name": "Windsor and Maidenhead",
        "organisation_entity": "377",
        "region": "South East",
        "portal_type": "api",
    },
    {
        "name": "Worcester",
        "organisation_entity": "378",
        "region": "West Midlands",
        "portal_type": "api",
    },
    {
        "name": "Woking",
        "organisation_entity": "379",
        "region": "South East",
        "portal_type": "api",
    },
    {
        "name": "Wokingham",
        "organisation_entity": "380",
        "region": "South East",
        "portal_type": "api",
    },
    {
        "name": "Worthing",
        "organisation_entity": "382",
        "region": "South East",
        "portal_type": "api",
    },
    {
        "name": "West Oxfordshire",
        "organisation_entity": "383",
        "region": "South East",
        "portal_type": "api",
    },
    {
        "name": "Wirral",
        "organisation_entity": "384",
        "region": "Merseyside",
        "portal_type": "api",
    },
    {
        "name": "Warrington",
        "organisation_entity": "385",
        "region": "North West",
        "portal_type": "api",
    },
    {
        "name": "West Suffolk",
        "organisation_entity": "386",
        "region": "East of England",
        "portal_type": "api",
    },
    {
        "name": "Westminster",
        "organisation_entity": "387",
        "region": "London",
        "portal_type": "idox",
        "portal_url": "https://idoxpa.westminster.gov.uk/online-applications",
    },
    {
        "name": "Wychavon",
        "organisation_entity": "390",
        "region": "West Midlands",
        "portal_type": "api",
    },
    {
        "name": "Wyre Forest",
        "organisation_entity": "392",
        "region": "West Midlands",
        "portal_type": "api",
    },
    {
        "name": "Wyre",
        "organisation_entity": "394",
        "region": "North West",
        "portal_type": "api",
    },
    {
        "name": "West Northamptonshire",
        "organisation_entity": "501909",
        "region": "East Midlands",
        "portal_type": "api",
    },
    {
        "name": "Westmorland and Furness",
        "organisation_entity": "10001",
        "region": "North West",
        "portal_type": "api",
    },
    # -------------------------------------------------------------------------
    # Y
    # -------------------------------------------------------------------------
    {
        "name": "York",
        "organisation_entity": "395",
        "region": "Yorkshire and the Humber",
        "portal_type": "api",
    },
    # -------------------------------------------------------------------------
    # Cumberland (new unitary)
    # -------------------------------------------------------------------------
    {
        "name": "Cumberland",
        "organisation_entity": "10000",
        "region": "North West",
        "portal_type": "api",
    },
]


# ---------------------------------------------------------------------------
# Helper lookups
# ---------------------------------------------------------------------------

# Entity code -> council dict
ENTITY_LOOKUP = {c["organisation_entity"]: c for c in ENGLISH_LPA_MAPPING}

# Simplified name (lower) -> council dict
NAME_LOOKUP = {c["name"].lower(): c for c in ENGLISH_LPA_MAPPING}

# Only councils with a portal scraper
PORTAL_COUNCILS = [c for c in ENGLISH_LPA_MAPPING if c.get("portal_url")]

# Group by portal type
IDOX_COUNCILS = [c for c in ENGLISH_LPA_MAPPING if c["portal_type"] == "idox"]
CIVICA_COUNCILS = [c for c in ENGLISH_LPA_MAPPING if c["portal_type"] == "civica"]
NEC_COUNCILS = [c for c in ENGLISH_LPA_MAPPING if c["portal_type"] == "nec"]
API_COUNCILS = [c for c in ENGLISH_LPA_MAPPING if c["portal_type"] == "api"]

# Group by region
REGIONS = {}
for _council in ENGLISH_LPA_MAPPING:
    REGIONS.setdefault(_council["region"], []).append(_council)
