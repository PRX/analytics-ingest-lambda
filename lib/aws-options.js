// just some shared aws-sdk defaults
const region = process.env.AWS_REGION || 'us-east-1';
const httpOptions = { connectTimeout: 1000, timeout: 2000 };
const clientOptions = { region, httpOptions, maxRetries: 3 };

module.exports = clientOptions;
