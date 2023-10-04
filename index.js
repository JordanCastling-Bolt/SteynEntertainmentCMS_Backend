const express = require('express');
const cors = require('cors');
const admin = require('firebase-admin');
const { BigQuery } = require('@google-cloud/bigquery');

const serviceAccount = require('./steynentertainment-800ea-firebase-adminsdk-oz4fr-cfc129dd25.json');

admin.initializeApp({
  credential: admin.credential.cert(serviceAccount),
});

const bigquery = new BigQuery({
  projectId: 'steynentertainment-800ea', 
  credentials: serviceAccount,
});

const app = express();
app.use(cors());
const port = 3001;

async function getLatestTable() {
  const dataset = bigquery.dataset('analytics_403555927');
  const [tables] = await dataset.getTables();

  // Filter out tables that don't start with 'pseudonymous_users_'
  const filteredTables = tables
    .map(t => t.id)
    .filter(tableName => tableName.startsWith('pseudonymous_users_'))
    .sort((a, b) => b.localeCompare(a));

  // Return the latest table
  return filteredTables[0];
}


async function runQuery(queryString, res, label) {
  try {
    const [rows] = await bigquery.query({ query: queryString });
    const result = {};
    result[label] = rows;
    res.json(result);
  } catch (error) {
    console.error(`Error running ${label} query`, error);
    res.status(500).send(error);
  }
}

app.get('/api/kpi/user', async (req, res) => {
  const latestTable = await getLatestTable();
  const query = `
    SELECT 
      user_id,
      user_pseudo_id
    FROM \`steynentertainment-800ea.analytics_403555927.${latestTable}\`
    LIMIT 100
  `;
  runQuery(query, res, 'users');
});

app.get('/api/kpi/geo', async (req, res) => {
  const latestTable = await getLatestTable();
  const query = `
    SELECT 
      geo,
      geo.city,
      geo.country
    FROM \`steynentertainment-800ea.analytics_403555927.${latestTable}\`
    ORDER BY last_updated_date DESC
    LIMIT 100
  `;
  runQuery(query, res, 'geo');
});

app.get('/api/kpi/mobile', async (req, res) => {
  const latestTable = await getLatestTable();
  const query = `
    SELECT 
      device,
      device.category,
      device.mobile_brand_name,
      device.operating_system
    FROM \`steynentertainment-800ea.analytics_403555927.${latestTable}\`
    ORDER BY last_updated_date DESC
    LIMIT 100
  `;
  runQuery(query, res, 'mobile');
});

app.listen(port, () => {
  console.log(`Server running on http://localhost:${port}`);
});
