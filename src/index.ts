import AWS from 'aws-sdk';
import stringify from 'csv-stringify';
import { Writable } from 'stream';

async function* iterateAllFiles(
  conn: AWS.S3,
  bucket: string,
  prefix: string,
): AsyncIterableIterator<string> {
  let continuationToken;
  while (true) {
    const resp = (await conn
      .listObjectsV2({
        Bucket: bucket,
        Prefix: prefix,
        ContinuationToken: continuationToken,
      })
      .promise()) as AWS.S3.ListObjectsV2Output;
    const content = resp.Contents;
    if (content == null) {
      return;
    }
    for (const elem of content) {
      if (elem.Key != null && elem.Size != null && elem.Size > 0) {
        yield elem.Key;
      }
    }
    if (resp.IsTruncated) {
      continuationToken = resp.ContinuationToken;
    } else {
      break;
    }
  }
}

const getContent = async (
  conn: AWS.S3,
  bucket: string,
  key: string,
): Promise<string | void> => {
  const resp = await conn
    .getObject({
      Bucket: bucket,
      Key: key,
    })
    .promise();
  const body = resp.Body;
  if (body == null) {
    return;
  }
  return body.toString();
};

interface AccessLogLine {
  inputKey: string;
  bucketOwner: string;
  bucket: string;
  time: string;
  remoteIP: string;
  requester: string;
  requestId: string;
  operation: string;
  key: string;
  requestURI: string;
  statusCode: string;
  errorCode: string;
  bytesSent: string;
  objectSize: string;
  turnAroundTime: string;
  referrer: string;
  userAgent: string;
  versionId: string;
}

const parseLine = (inputKey: string, line: string): AccessLogLine => {
  const [
    bucketOwner,
    bucket,
    time,
    timezone,
    remoteIP,
    requester,
    requestId,
    operation,
    key,
    requestURIMethod,
    requestURIPath,
    requestURIProtocol,
    statusCode,
    errorCode,
    bytesSent,
    objectSize,
    turnAroundTime,
    referrer,
    ...userAgentAndVersionId
  ] = line.split(' ');
  const versionId = userAgentAndVersionId[userAgentAndVersionId.length - 1];
  const userAgent = userAgentAndVersionId.slice(0, userAgentAndVersionId.length - 1).join(' ');
  return {
    inputKey,
    bucketOwner,
    bucket,
    time: time.slice(1) + timezone.slice(0, timezone.length - 1),
    remoteIP,
    requester,
    requestId,
    operation,
    key,
    requestURI:
      requestURIMethod.slice(1) +
      ' ' +
      requestURIPath +
      ' ' +
      requestURIProtocol.slice(0, requestURIProtocol.length - 1),
    statusCode,
    errorCode,
    bytesSent,
    objectSize,
    turnAroundTime,
    referrer,
    userAgent,
    versionId,
  };
};

const parseContent = (key: string, content: string): AccessLogLine[] => {
  const result: ReturnType<typeof parseContent> = [];
  for (const line of content.split('\n')) {
    const trimmedLine = line.trim();
    if (trimmedLine.length === 0) {
      continue;
    }
    result.push(parseLine(key, trimmedLine));
  }
  return result;
};

const writeLines = (lines: AccessLogLine[], output: Writable) => {
  stringify(lines, {
    header: true,
  }).pipe(output);
};

const main = async (
  accessKeyId: string,
  secretAccessKey: string,
  region: string,
  bucket: string,
  prefix: string,
) => {
  const conn = new AWS.S3({
    accessKeyId,
    secretAccessKey,
    region,
  });

  let lines: ReturnType<typeof parseContent> = [];
  for await (const key of iterateAllFiles(conn, bucket, prefix)) {
    const content = await getContent(conn, bucket, key);
    if (content == null) {
      continue;
    }
    lines = [...lines, ...parseContent(key, content)];
  }
  writeLines(lines, process.stdout);
};

if (module === require.main) {
  const args = process.argv.slice(2);
  if (args.length !== 5) {
    // tslint:disable-next-line:no-console
    console.error('Please supply the arguments: "accessKey" "secret" "region" "bucket" "prefix"');
    process.exit(1);
  }

  main(...args as [string, string, string, string, string]);
}
