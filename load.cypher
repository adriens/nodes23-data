MATCH (n)
DETACH DELETE n;
//dbms.security.allow_csv_import_from_file_urls=false;


LOAD CSV WITH HEADERS FROM 'file:///catalog_files-with-headers.csv' AS line
CREATE (:Dataset {
  dataset_id: line.dataset_id,
  filename: line.filename,
  gz_size_bytes: line.gz_size_bytes,
  gz_md5: line.gz_md5,
  size_bytes: line.size_bytes,
  md5: line.md5,
  creation_date: line.creation_date,
  headers: line.headers
  });
MATCH (d:Dataset)
RETURN d;

LOAD CSV WITH HEADERS FROM 'file:///publishers-with-headers.csv' AS line
CREATE (:Publisher {
  publisher: line.publisher,
  shortname: line.shortname,
  iso_alpha_2: line.iso_alpha_2,
  iso_alpha_3: line.iso_alpha_3,
  code_officiel_geographique_insee: line.code_officiel_geographique_insee,
  siren: line.siren,
  siret: line.siret,
  rid7: line.rid7,
  www_url: line.www_url,
  handle_twitter: line.handle_twitter,
  handle_facebook: line.handle_facebook,
  handle_instagram: line.handle_instagram,
  url_linkedin: line.url_linkedin,
  handle_youtube: line.handle_youtube,
  handle_github: line.handle_github,
  handle_dockerhub: line.handle_dockerhub,
  handle_kaggle: line.handle_kaggle
  });
MATCH (p:Publisher)
RETURN p;

LOAD CSV WITH HEADERS FROM 'file:///catalog_nc-with-headers.csv' AS line
CREATE (:CatalogItem {
  datasetid: line.datasetid,
  title: line.title,
  description: line.description,
  theme: line.theme,
  keywords: line.keywords,
  license: line.license,
  license_url: line.license_url,
  languages: line.languages,
  metadata_languages: line.metadata_languages,
  timezone: line.timezone,
  modified: line.modified,
  modified_updates_on_metadata_change: line.modified_updates_on_metadata_change,
  modified_updates_on_data_change: line.modified_updates_on_data_change,
  data_processed: line.data_processed,
  metadata_processed: line.metadata_processed,
  geographic_reference: line.geographic_reference,
  geographic_reference_auto: line.geographic_reference_auto,
  territory: line.territory,
  geometry_types: line.geometry_types,
  publisher: line.publisher,
  refs: line.refs,
  records_count: line.records_count,
  attributions: line.attributions,
  federated: line.federated,
  cycle: line.cycle,
  periodicity: line.periodicity,
  is_finalzed: line.is_finalzed
  });
MATCH (c:CatalogItem)
RETURN c;

LOAD CSV WITH HEADERS FROM 'file:///headers_all-with-headers.csv' AS line
CREATE (:DatasetHeader {
  dataset_id: line.dataset_id,
  header: line.header
  });
MATCH (h:DatasetHeader)
RETURN h;

LOAD CSV WITH HEADERS FROM 'file:///licences-with-headers.csv' AS line
CREATE (:License {
  cat: line.cat,
  id: line.id,
  category: line.category,
  description: line.description,
  license_deed_url: line.license_deed_url,
  legal_code_url: line.legal_code_url
  });

MATCH (l:License)
RETURN l;

LOAD CSV WITH HEADERS FROM 'file:///keywords_dict-with-headers.csv' AS line
CREATE (:Keyword {
  keyword: line.keyword
  });

MATCH (k:Keyword)
RETURN k;

LOAD CSV WITH HEADERS FROM 'file:///un_stats_sdg_goal_list-with-headers.csv' AS line
CREATE (:UnitedNationsSustainableDevelopmentGoal {
  code: line.code,
  title: line.title,
  description: line.description,
  uri: line.uri
  });

MATCH (g:UnitedNationsSustainableDevelopmentGoal)
RETURN g;

LOAD CSV WITH HEADERS FROM 'file:///un_stats_sdg_goal_indicator-with-headers.csv' AS line
CREATE (:UnitedNationsSustainableDevelopmentGoalIndicator {
  goal: line.goal,
  target: line.target,
  code: line.code,
  description: line.description,
  uri: line.uri
  });

MATCH (i:UnitedNationsSustainableDevelopmentGoalIndicator)
RETURN i;

// link Publishers.publisher with CatalogItem.publisher
MATCH (i:CatalogItem), (p:Publisher)
WHERE i.publisher = p.publisher
CREATE (i)-[rs:IS_PUBLISHED_BY]->(p);

// link UN Goalds and KPIs
MATCH (g:UnitedNationsSustainableDevelopmentGoal), (i:UnitedNationsSustainableDevelopmentGoalIndicator)
WHERE g.code = i.goal
CREATE (i)-[rs:IS_KPI_OF]->(g);

// link datasets and catalog together
MATCH (d:Dataset), (i:CatalogItem)
WHERE d.dataset_id = i.datasetid
CREATE (d)-[rs:IS_DATASET_OF]->(i);

// link catalog and license
MATCH (l:License), (i:CatalogItem)
WHERE l.id = i.license
CREATE (i)-[rs:IS_LICENSED_UNDER]->(l);

// link datasets and headers
MATCH (h:DatasetHeader), (d:Dataset)
WHERE h.dataset_id = d.dataset_id
CREATE (h)-[rs:IS_HEADER_OF]->(d);

// link Dataset and Keyword
MATCH (k:Keyword), (i:CatalogItem)
WHERE i.keywords CONTAINS k.keyword
CREATE (k)-[rs:IS_KEYWORD_OF]->(i);

// 
LOAD CSV WITH HEADERS FROM 'file:///entreprises_actives_au_ridet-with-headers.csv' AS line
CREATE (:EntrepriseActiveAuRidet {
  rid7: line.rid7,
  denomination: line.denomination,
  date_entreprise_active: line.date_entreprise_active,
   date_emploi: line.date_emploi,
   code_formjur: line.code_formjur,
   libelle_formjur: line.libelle_formjur,
   code_ape: line.code_ape,
   libelle_naf: line.libelle_naf,
   division_naf: line.division_naf,
   libelle_division_naf: line.libelle_division_naf,
   section_naf: line.section_naf,
   libelle_section_naf: line.libelle_section_naf,
   code_commune: line.code_commune,
   libelle_commune: line.libelle_commune,
   hors_nc: line.hors_nc,
   province: line.province,
   has_salaries: line.has_salaries
  });
MATCH (e:EntrepriseActiveAuRidet)
RETURN e;



LOAD CSV WITH HEADERS FROM 'file:///etablissements_actifs_au_ridet-with-headers.csv' AS line
CREATE (:EtablissementActifAuRidet {
  rid7: line.rid7,
  date_etablis_actif: line.date_etablis_actif,
  num_etablissement: line.num_etablissement,
  denomination: line.denomination,
  sigle: line.sigle,
  enseigne: line.enseigne,
  code_formjur: line.code_formjur,
  libelle_formjur: line.libelle_formjur,
  code_APE: line.code_APE,
  libelle_NAF: line.libelle_NAF,
  division_NAF: line.division_NAF,
  libelle_division_NAF: line.libelle_division_NAF,
  section_NAF: line.section_NAF,
  libelle_section_NAF: line.libelle_section_NAF,
  code_commune: line.code_commune,
  libelle_commune: line.libelle_commune,
  hors_NC: line.hors_NC,
  province: line.province
  });
MATCH (e:EtablissementActifAuRidet)
RETURN e;

// Link Entreprise and Etablissement
MATCH (e:EtablissementActifAuRidet), (p:EntrepriseActiveAuRidet)
WHERE p.rid7 = e.rid7
CREATE (e)-[rs:IS_ETABLISSEMENT_OF]->(p);

MATCH (e:EntrepriseActiveAuRidet), (p:Publisher)
WHERE p.rid7 = e.rid7
CREATE (p)-[rs:LINKED_TO_ENTERPRISE]->(e);

// Link Catalogtems and UN goals
LOAD CSV WITH HEADERS FROM "file:///catalog_nc_un_stats_sdg_goal-with-headers.csv" AS row
MATCH (i:CatalogItem), (g:UnitedNationsSustainableDevelopmentGoal)
WHERE
    i.datasetid = row.dataset_id AND
    g.code = row.un_stats_sdg_goal_code
CREATE (i)-[r:IMPLEMENTS_UNITED_NATION_GOAL]->(g)
RETURN r;

// Create Theme Nodes
LOAD CSV WITH HEADERS FROM 'file:///themes-with-headers.csv' AS line
CREATE (:Theme {
  id: line.id  });

MATCH (t:Theme)
RETURN t;

// Link CatalogItem w. theme
LOAD CSV WITH HEADERS FROM "file:///catalog_themes-with-headers.csv" AS row
MATCH (i:CatalogItem), (t:Theme)
WHERE
    i.datasetid = row.dataset_id AND
    t.id = row.theme
CREATE (i)-[r:IMPLEMENTS_THEME]->(t)
RETURN r;

LOAD CSV WITH HEADERS FROM 'file:///communes_nc_limites_terrestres_simplifiees-with-headers.csv' AS line
CREATE (:CommuneNC {
  code_officiel_geographique_insee: line.code_officiel_geographique_insee,
    objectid: line.objectid,
    nom_commune: line.nom_commune,
    code_postal: line.code_postal,
    nom_minuscule: line.nom_minuscule,
    geo_point: line.geo_point
  });
MATCH (c:CommuneNC)
RETURN c;

// Link publishers and communes
MATCH (p:Publisher), (c:CommuneNC)
WHERE
  p.code_officiel_geographique_insee = c.code_officiel_geographique_insee
CREATE (p)-[rs:LOCATED_IN]->(c);

// Load countries
LOAD CSV WITH HEADERS FROM 'file:///liste_pays_territoires_etrangers-with-headers.csv' AS line
CREATE (:Country {
  codeiso2: line.codeiso2,
    codeiso3: line.codeiso3,
    codenum3: line.codenum3,
    cog: line.cog,
    libcog: line.libcog,
    actual: line.actual,
    capay: line.capay,
    crpay: line.crpay,
    ani: line.ani,
    libenr: line.libenr,
    ancnom: line.ancnom
  });
MATCH (c:Country)
RETURN c;

// Link publisher and country
MATCH (p:Publisher), (c:Country)
WHERE
  p.iso_alpha_2 = c.codeiso2
CREATE (p)-[rs:FROM_COUNTRY]->(c);

// Create Header entities 
LOAD CSV WITH HEADERS FROM 'file:///headers_dict-with-headers.csv' AS line
CREATE (:Header {
  header: line.header
  });
MATCH (h:Header)
RETURN h;

// Link Header to CatalogItem/dataset with  
LOAD CSV WITH HEADERS FROM "file:///headers_all-with-headers.csv" AS row
MATCH (h:Header), (c:CatalogItem)
WHERE
    c.datasetid = row.dataset_id AND
    h.header = row.header 
CREATE (h)-[r:IS_HEADER_OF]->(c)
RETURN r;

// Load Pacific Data Hub Topics
LOAD CSV WITH HEADERS FROM 'file:///pacific_datahub_topics-with-headers.csv' AS line
CREATE (:PacificDataHubTopic {
  id: line.id,
  description: line.description,
  www: line.www
  });
MATCH (p:PacificDataHubTopic)
RETURN p;

// Link PacificDataHubTopic and UnitedNationsSustainableDevelopmentGoal
LOAD CSV WITH HEADERS FROM "file:///pacific_datahub_topics_un_stats_sdg_goal-with-headers.csv" AS row
MATCH (t:PacificDataHubTopic), (g:UnitedNationsSustainableDevelopmentGoal)
WHERE
    t.id = row.pacific_hub_topic AND
    g.code = row.un_stats_sdg_goal 
CREATE (t)-[r:MAPS_TO_UN_SDG_GOAL]->(g)
RETURN r;

// show graph
call apoc.meta.graph();
