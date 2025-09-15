// just some shared aws-sdk defaults
const region = process.env.AWS_REGION || "us-east-1";
const httpOptions = { connectTimeout: 1000, timeout: 2000 };
const clientOptions = { region, httpOptions, maxRetries: 3 };

// for local testing and development
if (process.env.DDB_LOCAL) {
  clientOptions.accessKeyId = "access";
  clientOptions.secretAccessKey = "secret";
  clientOptions.endpoint = `http://${process.env.DDB_LOCAL}:8000`;
  clientOptions.region = "local";
}

module.exports = clientOptions;
