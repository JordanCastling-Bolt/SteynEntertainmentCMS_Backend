const express = require('express');
const cors = require('cors');
const admin = require('firebase-admin');
const { BigQuery } = require('@google-cloud/bigquery');

const serviceAccount = require('./steynentertainment-800ea-firebase-adminsdk-oz4fr-cfc129dd25.json');

admin.initializeApp({
  credential: admin.credential.cert(serviceAccount),
});

const bigquery = new BigQuery({
  projectId: 'steynentertainment-800ea', // Replace with your project ID
  credentials: serviceAccount,
});

const app = express();
app.use(cors());
const port = 3001;

// Query for user data
app.get('/api/kpi/user', async (req, res) => {
  const query = `
    SELECT 
      user_id,
      user_pseudo_id
    FROM \`steynentertainment-800ea.analytics_403555927.events_20230929\`
  `;
  try {
    const [rows] = await bigquery.query({ query });
    res.json({ users: rows });
  } catch (error) {
    console.error('Error running user query', error);
    res.status(500).send(error);
  }
});

// Query for geo data
app.get('/api/kpi/geo', async (req, res) => {
  const query = `
    SELECT 
      geo
    FROM \`steynentertainment-800ea.analytics_403555927.events_20230929\`
  `;
  try {
    const [rows] = await bigquery.query({ query });
    res.json({ geo: rows });
  } catch (error) {
    console.error('Error running geo query', error);
    res.status(500).send(error);
  }
});

// Query for mobile data
app.get('/api/kpi/mobile', async (req, res) => {
  const query = `
  SELECT 
  device,
    device.category,
    device.mobile_brand_name,
    device.operating_system
  FROM \`steynentertainment-800ea.analytics_403555927.events_20230929\`
`;

  try {
    const [rows] = await bigquery.query({ query });
    res.json({ mobile: rows });
  } catch (error) {
    console.error('Error running mobile query', error);
    res.status(500).send(error);
  }
});

// Start the server
app.listen(port, () => {
  console.log(`Server running on http://localhost:${port}`);
});
