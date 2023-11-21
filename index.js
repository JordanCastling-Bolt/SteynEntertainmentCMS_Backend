const express = require('express');
const cors = require('cors');
const https = require('https');
const fs = require('fs');
const admin = require('firebase-admin');
const { BigQuery } = require('@google-cloud/bigquery');
const NodeCache = require('node-cache');
const cache = new NodeCache();
const serviceAccount = require('./steynentertainment-800ea-firebase-adminsdk-oz4fr-cfc129dd25.json');

admin.initializeApp({
  credential: admin.credential.cert(serviceAccount),
});

const options = {
  key: fs.readFileSync('keys/mydomain.key'),
  cert: fs.readFileSync('keys/mydomain.crt')
};

const datasetId = 'analytics_403555927';
const bigquery = new BigQuery({
  projectId: 'steynentertainment-800ea',
  credentials: serviceAccount,
});
const dataset = bigquery.dataset(datasetId);

const app = express();
app.use(cors());
const port = 3001;

app.use((req, res, next) => {
  res.header('Access-Control-Allow-Origin', '*');
  res.header('Access-Control-Allow-Headers', 'Origin, X-Requested-With, Content-Type, Accept');
  next();
});


function getDateRangeTableName(range) {
  const today = new Date();
  switch (range) {
    case '7days': {
      const weekAgo = new Date(today - 7 * 24 * 60 * 60 * 1000);
      return `pseudonymous_users_${weekAgo.getFullYear()}${(weekAgo.getMonth() + 1).toString().padStart(2, '0')}${weekAgo.getDate().toString().padStart(2, '0')}`;
    }
    case '3months': {
      const threeMonthsAgo = new Date(today - 90 * 24 * 60 * 60 * 1000);
      return `pseudonymous_users_${threeMonthsAgo.getFullYear()}${(threeMonthsAgo.getMonth() + 1).toString().padStart(2, '0')}${threeMonthsAgo.getDate().toString().padStart(2, '0')}`;
    }
    default:
      return null;
  }
}


async function getLatestTable(range) {
  const [tables] = await dataset.getTables();

  const filteredTables = tables
    .map(t => t.id)
    .filter(tableName => tableName.startsWith('events_'))
    .sort((a, b) => b.localeCompare(a));

  if (range) {
    const rangeTable = getDateRangeTableName(range);
    if (filteredTables.includes(rangeTable)) {
      return rangeTable;
    }
    // Handle case where specific range table isn't found
    throw new Error(`Table for range ${range} not found.`);
  }

  // Return the latest table
  return filteredTables[0];
}
async function runQuery(queryString, res, label) {
  try {
    if (cache.has(queryString)) {
      const cachedData = cache.get(queryString);
      res.json(cachedData.map(item => item[label]));
    } else {
      const [rows] = await bigquery.query({ query: queryString });
      const result = rows.map(row => ({ [label]: row }));
      cache.set(queryString, result);
      res.json(result);
    }
  } catch (error) {
    console.error(`Error running ${label} query`, error);
    res.status(500).send(error);
  }
}

const createKPIRoute = (endpoint, querySelector, responseLabel) => {
  app.get(`/api/kpi/${endpoint}`, async (req, res) => {
    try {
      const latestTable = await getLatestTable();
      const query = querySelector(latestTable);
      runQuery(query, res, responseLabel);
    } catch (error) {
      console.error(`Error fetching ${responseLabel}`, error);
      res.status(500).send(error);
    }
  });
};

const querySelectors = {
  user: ()=> `
    SELECT 
      user_pseudo_id,
      COUNT(is_active_user) as active_count
    FROM \`${datasetId}.events_*\`
    GROUP BY user_pseudo_id
  `,
  geo: ()=>`
    WITH RankedEvents AS (
      SELECT *,
             ROW_NUMBER() OVER (PARTITION BY user_pseudo_id ORDER BY event_timestamp DESC) as rn
      FROM \`${datasetId}.events_*\`
    )
    SELECT 
      user_pseudo_id,
      geo,
      geo.city,
      geo.country,
      geo.region
    FROM RankedEvents
    WHERE rn = 1
    LIMIT 100
  `,
  mobile: ()=>`
    SELECT 
      device,
      device.category,
      device.mobile_brand_name,
      device.mobile_model_name,
      device.operating_system
    FROM \`${datasetId}.events_*\`
    ORDER BY event_timestamp DESC
    LIMIT 100
  `,
  userEngagement:()=>`
    SELECT 
      event_name,
      COUNT(event_name) as event_count
    FROM \`${datasetId}.events_*\`
    GROUP BY event_name
  `,
  technology: ()=>`
    SELECT 
      device.browser,
      COUNT(device.browser) as browser_count,
      device.operating_system,
      COUNT(device.operating_system) as os_count
    FROM \`${datasetId}.events_*\`
    GROUP BY device.browser, device.operating_system
  `,
  acquisition:()=>`
    SELECT 
      traffic_source.source,
      COUNT(traffic_source.source) as source_count,
      traffic_source.medium,
      COUNT(traffic_source.medium) as medium_count
    FROM \`${datasetId}.events_*\`
    GROUP BY traffic_source.source, traffic_source.medium
  `,
  behaviorFlow: ()=>`
    SELECT 
      event_name,
      event_bundle_sequence_id
    FROM \`${datasetId}.events_*\`
    ORDER BY event_bundle_sequence_id
    LIMIT 1000
  `,
  userRetention:()=>`
    SELECT DATE(TIMESTAMP_MICROS(event_timestamp)) as date, COUNT(DISTINCT user_pseudo_id) as retained_users
    FROM \`${datasetId}.events_*\`
    WHERE is_active_user = True
    GROUP BY date
    ORDER BY date DESC
    LIMIT 30
  `,
  eventPopularity: ()=> `
    SELECT 
      event_name,
      COUNT(event_name) as event_count
    FROM \`${datasetId}.events_*\`
    GROUP BY event_name
    ORDER BY event_count DESC
    LIMIT 10
  `,
  trafficSourceAnalysis: ()=>`
    SELECT 
      traffic_source.source,
      traffic_source.medium,
      traffic_source.name,
      COUNT(traffic_source.source) as source_count,
      COUNT(traffic_source.medium) as medium_count,
      COUNT(traffic_source.name) as name_count
    FROM \`${datasetId}.events_*\`
    GROUP BY traffic_source.source, traffic_source.medium, traffic_source.name
  `,
  getUserActivityOverTime: ()=>`
  SELECT 
  DATE(TIMESTAMP_MICROS(event_timestamp)) as date, 
  user_pseudo_id, 
  COUNT(*) as active_count
  FROM \`${datasetId}.events_*\`
    GROUP BY date, user_pseudo_id
  ORDER BY date DESC
  LIMIT 100; 
  `,
};

// Example usage:
createKPIRoute('user', querySelectors.user, 'user');
createKPIRoute('geo', querySelectors.geo, 'geo');
createKPIRoute('mobile', querySelectors.mobile, 'mobile');
createKPIRoute('userEngagement', querySelectors.userEngagement, 'userEngagement');
createKPIRoute('technology', querySelectors.technology, 'technology');
createKPIRoute('acquisition', querySelectors.acquisition, 'acquisition');
createKPIRoute('behaviorFlow', querySelectors.behaviorFlow, 'behaviorFlow');
createKPIRoute('userRetention', querySelectors.userRetention, 'userRetention');
createKPIRoute('eventPopularity', querySelectors.eventPopularity, 'eventPopularity');
createKPIRoute('trafficSourceAnalysis', querySelectors.trafficSourceAnalysis, 'trafficSourceAnalysis');
createKPIRoute('userActivityOverTime', querySelectors.getUserActivityOverTime, 'userActivityOverTime');

https.createServer(options, app).listen(port, () => {
  console.log(`Server running on https://localhost:${port}`);
});

