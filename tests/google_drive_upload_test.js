const { createReadStream } = require('fs');
const path = require('path');
const process = require('process');
const { google } = require('googleapis');

// Downloaded from while creating credentials of service account
const pkey = {
  type: 'service_account',
  project_id: 'fluxdb-403713',
  private_key_id: '951aef0770a84907516a4d5310140050cdb02091',
  private_key: '-----BEGIN PRIVATE KEY-----\nMIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQDOEcelc87fRyHJ\njqgaPP6jqdRAl2PGTKe42csJoPNeOx0HLHQgcehGKecfuW8ab/n0k1K7my6YE0KZ\nf07EPC2Hv4dowHjqIFD8I/QG9den7eodG11QPHWxkOF0AxmnJhnq1/wCh+YnT4BP\nkXIdT3l5tViXxVXhrlbso9xjP7vRdus7pz9om1Y3RKrRjXLf2bazP+sUCWvzlLdi\ni/vF8CdEgwvcpF+XjHvKTne/aYLYbDWm2DYvJku2VGzrwP1mc1bbwWkanQ3NBFB5\nmwgBQI+WUcpPl8h9fzjluAnXV2DXT75k/5J2JdQOi7qlPm2z2LR2LFaAKWSuTLvW\nSk+LH12rAgMBAAECggEAC+3VOrDitmPbQwbqQ06UR8F9OISrBsqJi7a2krxJqlhO\nB2Fy6C0/5CPpLcR3IowokkMfBHOTdcPo/z1ylViNzYXFtHIumGW+L8JzDh4YLwVO\nSq4QBPo4WUrzn4V4bDJ/80DakAOcTAVFqRWDNMrU049Zwrtui83Nrrg+T1PcBGA9\nMaimgMifZsASdFbaqonBF4kj8K4HG22Y+R66rD0RdvjqTbGsNjiuSClzLKvs06lb\nnXLMlntoZa9BHIyU4ytSsnzW5cEQOJJHOZVyoQrPgvFu/kl1+jjWb+MpQYH1P6cM\naqWuLq5ZvH+MXPyEe/fNoYPXvzt2VaduUcO91AsvYQKBgQDroajwdjj2EGA1o9m8\nFqYJzhZh5xc+iilbsqa17Y+cthYJ0CU5IdWnCwlZ93lUXq5Vs5mPEYARAiynr3es\n8ocZc/Pkwfz40lRdBRU1n6RYNzyl6U1mgdNu7/WJsriwogR6OeP/oPrmiszxr0cU\n7AfwBGjZwnGeVI7lmzTQJy2m4QKBgQDf4e9zy1E8JlfXqEQdzFsvqLhX32HeI7Nr\nhLrU+rUFaI5p2FC7VaK70Dx9fYDatQUUP1T2inUxN4NuM8VpE5s6+lFAvP15G34t\nYg5N1TQ3yvvGV8wRl+I4u3mGVvGoWc1QZwWFELb4+T0lkmka9eIuZaFwL2SPC2d9\nSB1GF/lyCwKBgQDXveN1j3kl6uZ8FnKUYVRTI+ugZjsFGvE1MUSszD8yqBFTmM2M\nGuuJD3TXd9wSIMKUW2Xc9ZQBfrEuM11q74A9EMqdh/Q8Si/OH4pE189cqe6QpiUl\nFvdk2rZBBm9N4nohAwI7msQ+85UkMGzvvhCidRcfUoY/BoLzvYDEmSqWYQKBgEr8\nwxZW0FPESAHcw0vzycsRyQTttjsQXCU0JNv3STCRj7nWEVxd147utYQWyFT48sWQ\nXOXjBFPC00vTHVkPjxvXAeYcJw4sbjvHLyFUuxPA8knB2IFADS6RZKfhcTnBMmqu\nxwOF0LNdz+RIwNmd5+9AbS1FQnpDCJvGzr9Ogmd3AoGBAONq1eYkfUT0ioVyLnBA\n9LNB1bLhTadDBXmhF+c2lGGH2eHr3cdTkOlEwWChl244U7lqdi7Jz0QY1y2Xd37n\nlg2lMuCHGf5Sx3UNsh4CHdIsJXtMXid6aYigT4I3bIFbNSSZYTbMLz20J67+hYq9\nF+7UKUByhFH+42ZpLZ9nIX7w\n-----END PRIVATE KEY-----\n',
  client_email: 'fluxdbtest@fluxdb-403713.iam.gserviceaccount.com',
  client_id: '117838168227552862093',
  auth_uri: 'https://accounts.google.com/o/oauth2/auth',
  token_uri: 'https://oauth2.googleapis.com/token',
  auth_provider_x509_cert_url: 'https://www.googleapis.com/oauth2/v1/certs',
  client_x509_cert_url: 'https://www.googleapis.com/robot/v1/metadata/x509/fluxdbtest%40fluxdb-403713.iam.gserviceaccount.com',
  universe_domain: 'googleapis.com',
};

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
