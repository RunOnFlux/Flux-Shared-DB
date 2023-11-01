const { createReadStream } = require('fs');
const path = require('path');
const process = require('process');
const { google } = require('googleapis');

// Downloaded from while creating credentials of service account
const pkey = {};

const SCOPES = ['https://www.googleapis.com/auth/drive.file'];

/**
 * Authorize with service account and get jwt client
 *
 */
async function authorize() {
  const jwtClient = new google.auth.JWT(
    pkey.client_email,
    null,
    pkey.private_key,
    SCOPES,
  );
  await jwtClient.authorize();
  return jwtClient;
}

/**
 * Create a new file on google drive.
 * @param {OAuth2Client} authClient An authorized OAuth2 client.
 */
async function uploadFile(authClient) {
  const drive = google.drive({ version: 'v3', auth: authClient });
  const file = await drive.files.create({
    media: {
      body: createReadStream('./tests/sql/test.sql'),
    },
    fields: 'id',
    requestBody: {
      name: path.basename('test.sql'),
    },
  });
  console.log(file.data.id);
}

async function listFiles(authClient) {
  const drive = google.drive({ version: 'v3', auth: authClient });
  const res = await drive.files.list({
    pageSize: 10,
    fields: 'nextPageToken, files(id, name, mimeType, createdTime)',
  });
  const files = res.data.files;
  if (files.length === 0) {
    console.log('No files found.');
    return;
  }

  console.log('Files:');
  files.map((file) => {
    console.log(`${JSON.stringify(file)}`);
  });
}

authorize().then(listFiles).catch(console.error);
