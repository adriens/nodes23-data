MATCH (n)
DETACH DELETE n;
//dbms.security.allow_csv_import_from_file_urls=false;


LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/adriens/nodes23-data/main//catalog_files-with-headers.csv' AS line
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

LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/adriens/nodes23-data/main//publishers-with-headers.csv' AS line
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

LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/adriens/nodes23-data/main//catalog_nc-with-headers.csv' AS line
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

LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/adriens/nodes23-data/main//licences-with-headers.csv' AS line
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

LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/adriens/nodes23-data/main//un_stats_sdg_goal_list-with-headers.csv' AS line
CREATE (:UnitedNationsSustainableDevelopmentGoal {
  code: line.code,
  title: line.title,
  description: line.description,
  uri: line.uri
  });

MATCH (g:UnitedNationsSustainableDevelopmentGoal)
RETURN g;

LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/adriens/nodes23-data/main//un_stats_sdg_goal_indicator-with-headers.csv' AS line
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


// Link Catalogtems and UN goals
LOAD CSV WITH HEADERS FROM "https://raw.githubusercontent.com/adriens/nodes23-data/main//catalog_nc_un_stats_sdg_goal-with-headers.csv" AS row
MATCH (i:CatalogItem), (g:UnitedNationsSustainableDevelopmentGoal)
WHERE
    i.datasetid = row.dataset_id AND
    g.code = row.un_stats_sdg_goal_code
CREATE (i)-[r:IMPLEMENTS_UNITED_NATION_GOAL]->(g)
RETURN r;

// Link CatalogItem w. theme
LOAD CSV WITH HEADERS FROM "https://raw.githubusercontent.com/adriens/nodes23-data/main//catalog_themes-with-headers.csv" AS row
MATCH (i:CatalogItem), (t:Theme)
WHERE
    i.datasetid = row.dataset_id AND
    t.id = row.theme
CREATE (i)-[r:IMPLEMENTS_THEME]->(t)
RETURN r;


// Load Pacific Data Hub Topics
LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/adriens/nodes23-data/main//pacific_datahub_topics-with-headers.csv' AS line
CREATE (:PacificDataHubTopic {
  id: line.id,
  description: line.description,
  www: line.www
  });
MATCH (p:PacificDataHubTopic)
RETURN p;

// Link PacificDataHubTopic and UnitedNationsSustainableDevelopmentGoal
LOAD CSV WITH HEADERS FROM "https://raw.githubusercontent.com/adriens/nodes23-data/main//pacific_datahub_topics_un_stats_sdg_goal-with-headers.csv" AS row
MATCH (t:PacificDataHubTopic), (g:UnitedNationsSustainableDevelopmentGoal)
WHERE
    t.id = row.pacific_hub_topic AND
    g.code = row.un_stats_sdg_goal 
CREATE (t)-[r:MAPS_TO_UN_SDG_GOAL]->(g)
RETURN r;

LOAD CSV WITH HEADERS FROM "https://raw.githubusercontent.com/adriens/nodes23-data/main//un_stats_sdg_goal_dependencies-with-headers.csv" AS row
MATCH (g1:UnitedNationsSustainableDevelopmentGoal), (g2:UnitedNationsSustainableDevelopmentGoal)
WHERE
    g1.code = row.source AND
    g2.code = row.target
CREATE (g1)-[r:SUB_SDG_GOAL_OF]->(g2)
RETURN r;


// show graph
call apoc.meta.graph();
