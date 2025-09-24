// smithy-typescript generated code
import {
  BatchDeleteCommandInput,
  BatchDeleteCommandOutput,
} from "../commands/BatchDeleteCommand";
import {
  DeleteBucketIndexesCommandInput,
  DeleteBucketIndexesCommandOutput,
} from "../commands/DeleteBucketIndexesCommand";
import {
  DeleteObjectFromExpirationCommandInput,
  DeleteObjectFromExpirationCommandOutput,
} from "../commands/DeleteObjectFromExpirationCommand";
import {
  GetBucketCseqCommandInput,
  GetBucketCseqCommandOutput,
} from "../commands/GetBucketCseqCommand";
import {
  GetBucketIndexesCommandInput,
  GetBucketIndexesCommandOutput,
} from "../commands/GetBucketIndexesCommand";
import {
  GetBucketMetadataCommandInput,
  GetBucketMetadataCommandOutput,
} from "../commands/GetBucketMetadataCommand";
import {
  GetMetadataCommandInput,
  GetMetadataCommandOutput,
} from "../commands/GetMetadataCommand";
import {
  GetObjectCommandInput,
  GetObjectCommandOutput,
} from "../commands/GetObjectCommand";
import {
  GetObjectListCommandInput,
  GetObjectListCommandOutput,
} from "../commands/GetObjectListCommand";
import {
  GetRaftBucketsCommandInput,
  GetRaftBucketsCommandOutput,
} from "../commands/GetRaftBucketsCommand";
import {
  GetRaftIdCommandInput,
  GetRaftIdCommandOutput,
} from "../commands/GetRaftIdCommand";
import {
  GetRaftLogCommandInput,
  GetRaftLogCommandOutput,
} from "../commands/GetRaftLogCommand";
import {
  ListLifecycleCurrentsCommandInput,
  ListLifecycleCurrentsCommandOutput,
} from "../commands/ListLifecycleCurrentsCommand";
import {
  ListLifecycleNonCurrentsCommandInput,
  ListLifecycleNonCurrentsCommandOutput,
} from "../commands/ListLifecycleNonCurrentsCommand";
import {
  ListLifecycleOrphansCommandInput,
  ListLifecycleOrphansCommandOutput,
} from "../commands/ListLifecycleOrphansCommand";
import {
  MultipleBackendAbortMPUCommandInput,
  MultipleBackendAbortMPUCommandOutput,
} from "../commands/MultipleBackendAbortMPUCommand";
import {
  MultipleBackendCompleteMPUCommandInput,
  MultipleBackendCompleteMPUCommandOutput,
} from "../commands/MultipleBackendCompleteMPUCommand";
import {
  MultipleBackendDeleteObjectCommandInput,
  MultipleBackendDeleteObjectCommandOutput,
} from "../commands/MultipleBackendDeleteObjectCommand";
import {
  MultipleBackendDeleteObjectTaggingCommandInput,
  MultipleBackendDeleteObjectTaggingCommandOutput,
} from "../commands/MultipleBackendDeleteObjectTaggingCommand";
import {
  MultipleBackendHeadObjectCommandInput,
  MultipleBackendHeadObjectCommandOutput,
} from "../commands/MultipleBackendHeadObjectCommand";
import {
  MultipleBackendInitiateMPUCommandInput,
  MultipleBackendInitiateMPUCommandOutput,
} from "../commands/MultipleBackendInitiateMPUCommand";
import {
  MultipleBackendPutMPUPartCommandInput,
  MultipleBackendPutMPUPartCommandOutput,
} from "../commands/MultipleBackendPutMPUPartCommand";
import {
  MultipleBackendPutObjectCommandInput,
  MultipleBackendPutObjectCommandOutput,
} from "../commands/MultipleBackendPutObjectCommand";
import {
  MultipleBackendPutObjectTaggingCommandInput,
  MultipleBackendPutObjectTaggingCommandOutput,
} from "../commands/MultipleBackendPutObjectTaggingCommand";
import {
  PutBucketIndexesCommandInput,
  PutBucketIndexesCommandOutput,
} from "../commands/PutBucketIndexesCommand";
import {
  PutDataCommandInput,
  PutDataCommandOutput,
} from "../commands/PutDataCommand";
import {
  PutMetadataCommandInput,
  PutMetadataCommandOutput,
} from "../commands/PutMetadataCommand";
import { BackbeatServiceException as __BaseException } from "../models/BackbeatServiceException";
import { BatchDeleteLocation } from "../models/models_0";
import {
  loadRestJsonErrorCode,
  parseJsonBody as parseBody,
  parseJsonErrorBody as parseErrorBody,
} from "@aws-sdk/core";
import { requestBuilder as rb } from "@smithy/core";
import {
  HttpRequest as __HttpRequest,
  HttpResponse as __HttpResponse,
} from "@smithy/protocol-http";
import {
  expectBoolean as __expectBoolean,
  expectInt32 as __expectInt32,
  expectLong as __expectLong,
  expectNonNull as __expectNonNull,
  expectObject as __expectObject,
  expectString as __expectString,
  extendedEncodeURIComponent as __extendedEncodeURIComponent,
  parseRfc7231DateTime as __parseRfc7231DateTime,
  resolvedPath as __resolvedPath,
  _json,
  collectBody,
  isSerializableHeaderValue,
  map,
  take,
  withBaseException,
} from "@smithy/smithy-client";
import {
  DocumentType as __DocumentType,
  Endpoint as __Endpoint,
  ResponseMetadata as __ResponseMetadata,
  SerdeContext as __SerdeContext,
} from "@smithy/types";

/**
 * serializeAws_restJson1BatchDeleteCommand
 */
export const se_BatchDeleteCommand = async(
  input: BatchDeleteCommandInput,
  context: __SerdeContext
): Promise<__HttpRequest> => {
  const b = rb(input, context);
  const headers: any = map({}, isSerializableHeaderValue, {
    'content-type': 'application/json',
    [_ius]: input[_IUS]!,
    [_xssc]: input[_SC]!,
    [_xst]: input[_T]!,
    [_xsct]: input[_CT]!,
  });
  b.bp("/_/backbeat/batchdelete/{Bucket}/{Key}");
  b.p('Bucket', () => input.Bucket!, '{Bucket}', false)
  b.p('Key', () => input.Key!, '{Key}', false)
  let body: any;
  body = JSON.stringify(take(input, {
    'Locations': _ => _json(_),
  }));
  b.m("POST")
  .h(headers)
  .b(body);
  return b.build();
}

/**
 * serializeAws_restJson1DeleteBucketIndexesCommand
 */
export const se_DeleteBucketIndexesCommand = async(
  input: DeleteBucketIndexesCommandInput,
  context: __SerdeContext
): Promise<__HttpRequest> => {
  const b = rb(input, context);
  const headers: any = {
    'content-type': 'application/octet-stream',
  };
  b.bp("/_/backbeat/index/{Bucket}");
  b.p('Bucket', () => input.Bucket!, '{Bucket}', false)
  const query: any = map({
    [_o]: [, "delete"],
  });
  let body: any;
  if (input.Body !== undefined) {
    body = input.Body;
  }
  b.m("POST")
  .h(headers)
  .q(query)
  .b(body);
  return b.build();
}

/**
 * serializeAws_restJson1DeleteObjectFromExpirationCommand
 */
export const se_DeleteObjectFromExpirationCommand = async(
  input: DeleteObjectFromExpirationCommandInput,
  context: __SerdeContext
): Promise<__HttpRequest> => {
  const b = rb(input, context);
  const headers: any = {
  };
  b.bp("/_/backbeat/expiration/{Bucket}/{Key+}");
  b.p('Bucket', () => input.Bucket!, '{Bucket}', false)
  b.p('Key', () => input.Key!, '{Key+}', true)
  const query: any = map({
    [_vI]: [,input[_VI]!],
  });
  let body: any;
  b.m("DELETE")
  .h(headers)
  .q(query)
  .b(body);
  return b.build();
}

/**
 * serializeAws_restJson1GetBucketCseqCommand
 */
export const se_GetBucketCseqCommand = async(
  input: GetBucketCseqCommandInput,
  context: __SerdeContext
): Promise<__HttpRequest> => {
  const b = rb(input, context);
  const headers: any = {
  };
  b.bp("/_/metadata/default/informations/{Bucket}");
  b.p('Bucket', () => input.Bucket!, '{Bucket}', false)
  let body: any;
  b.m("GET")
  .h(headers)
  .b(body);
  return b.build();
}

/**
 * serializeAws_restJson1GetBucketIndexesCommand
 */
export const se_GetBucketIndexesCommand = async(
  input: GetBucketIndexesCommandInput,
  context: __SerdeContext
): Promise<__HttpRequest> => {
  const b = rb(input, context);
  const headers: any = {
  };
  b.bp("/_/backbeat/index/{Bucket}");
  b.p('Bucket', () => input.Bucket!, '{Bucket}', false)
  let body: any;
  b.m("GET")
  .h(headers)
  .b(body);
  return b.build();
}

/**
 * serializeAws_restJson1GetBucketMetadataCommand
 */
export const se_GetBucketMetadataCommand = async(
  input: GetBucketMetadataCommandInput,
  context: __SerdeContext
): Promise<__HttpRequest> => {
  const b = rb(input, context);
  const headers: any = {
  };
  b.bp("/_/metadata/default/attributes/{Bucket}");
  b.p('Bucket', () => input.Bucket!, '{Bucket}', false)
  let body: any;
  b.m("GET")
  .h(headers)
  .b(body);
  return b.build();
}

/**
 * serializeAws_restJson1GetMetadataCommand
 */
export const se_GetMetadataCommand = async(
  input: GetMetadataCommandInput,
  context: __SerdeContext
): Promise<__HttpRequest> => {
  const b = rb(input, context);
  const headers: any = {
  };
  b.bp("/_/backbeat/metadata/{Bucket}/{Key+}");
  b.p('Bucket', () => input.Bucket!, '{Bucket}', false)
  b.p('Key', () => input.Key!, '{Key+}', true)
  const query: any = map({
    [_vI]: [,input[_VI]!],
  });
  let body: any;
  b.m("GET")
  .h(headers)
  .q(query)
  .b(body);
  return b.build();
}

/**
 * serializeAws_restJson1GetObjectCommand
 */
export const se_GetObjectCommand = async(
  input: GetObjectCommandInput,
  context: __SerdeContext
): Promise<__HttpRequest> => {
  const b = rb(input, context);
  const headers: any = map({}, isSerializableHeaderValue, {
    [_xsci]: input[_CID]!,
  });
  b.bp("/{Bucket}/{Key+}");
  b.p('Bucket', () => input.Bucket!, '{Bucket}', false)
  b.p('Key', () => input.Key!, '{Key+}', true)
  const query: any = map({
    [_vI]: [,input[_VI]!],
  });
  let body: any;
  b.m("GET")
  .h(headers)
  .q(query)
  .b(body);
  return b.build();
}

/**
 * serializeAws_restJson1GetObjectListCommand
 */
export const se_GetObjectListCommand = async(
  input: GetObjectListCommandInput,
  context: __SerdeContext
): Promise<__HttpRequest> => {
  const b = rb(input, context);
  const headers: any = {
  };
  b.bp("/_/metadata/default/bucket/{Bucket}");
  b.p('Bucket', () => input.Bucket!, '{Bucket}', false)
  let body: any;
  b.m("GET")
  .h(headers)
  .b(body);
  return b.build();
}

/**
 * serializeAws_restJson1GetRaftBucketsCommand
 */
export const se_GetRaftBucketsCommand = async(
  input: GetRaftBucketsCommandInput,
  context: __SerdeContext
): Promise<__HttpRequest> => {
  const b = rb(input, context);
  const headers: any = {
  };
  b.bp("/_/metadata/admin/raft_sessions/{LogId}/bucket");
  b.p('LogId', () => input.LogId!, '{LogId}', false)
  let body: any;
  b.m("GET")
  .h(headers)
  .b(body);
  return b.build();
}

/**
 * serializeAws_restJson1GetRaftIdCommand
 */
export const se_GetRaftIdCommand = async(
  input: GetRaftIdCommandInput,
  context: __SerdeContext
): Promise<__HttpRequest> => {
  const b = rb(input, context);
  const headers: any = {
  };
  b.bp("/_/metadata/admin/buckets/{Bucket}/id");
  b.p('Bucket', () => input.Bucket!, '{Bucket}', false)
  let body: any;
  b.m("GET")
  .h(headers)
  .b(body);
  return b.build();
}

/**
 * serializeAws_restJson1GetRaftLogCommand
 */
export const se_GetRaftLogCommand = async(
  input: GetRaftLogCommandInput,
  context: __SerdeContext
): Promise<__HttpRequest> => {
  const b = rb(input, context);
  const headers: any = {
  };
  b.bp("/_/metadata/admin/raft_sessions/{LogId}/log");
  b.p('LogId', () => input.LogId!, '{LogId}', false)
  const query: any = map({
    [_b]: [() => input.Begin !== void 0, () => (input[_B]!.toString())],
    [_l]: [() => input.Limit !== void 0, () => (input[_L]!.toString())],
    [_tL]: [() => input.TargetLeader !== void 0, () => (input[_TL]!.toString())],
  });
  let body: any;
  b.m("GET")
  .h(headers)
  .q(query)
  .b(body);
  return b.build();
}

/**
 * serializeAws_restJson1ListLifecycleCurrentsCommand
 */
export const se_ListLifecycleCurrentsCommand = async(
  input: ListLifecycleCurrentsCommandInput,
  context: __SerdeContext
): Promise<__HttpRequest> => {
  const b = rb(input, context);
  const headers: any = {
  };
  b.bp("/_/backbeat/lifecycle/{Bucket}");
  b.p('Bucket', () => input.Bucket!, '{Bucket}', false)
  const query: any = map({
    [_lt]: [, "current"],
    [_bd]: [,input[_BD]!],
    [_edsn]: [,input[_EDSN]!],
    [_et]: [,input[_ET]!],
    [_m]: [,input[_M]!],
    [_mk]: [() => input.MaxKeys !== void 0, () => (input[_MK]!.toString())],
    [_p]: [,input[_P]!],
  });
  let body: any;
  b.m("GET")
  .h(headers)
  .q(query)
  .b(body);
  return b.build();
}

/**
 * serializeAws_restJson1ListLifecycleNonCurrentsCommand
 */
export const se_ListLifecycleNonCurrentsCommand = async(
  input: ListLifecycleNonCurrentsCommandInput,
  context: __SerdeContext
): Promise<__HttpRequest> => {
  const b = rb(input, context);
  const headers: any = {
  };
  b.bp("/_/backbeat/lifecycle/{Bucket}");
  b.p('Bucket', () => input.Bucket!, '{Bucket}', false)
  const query: any = map({
    [_lt]: [, "noncurrent"],
    [_bd]: [,input[_BD]!],
    [_edsn]: [,input[_EDSN]!],
    [_et]: [,input[_ET]!],
    [_km]: [,input[_KM]!],
    [_vim]: [,input[_VIM]!],
    [_mk]: [() => input.MaxKeys !== void 0, () => (input[_MK]!.toString())],
    [_p]: [,input[_P]!],
  });
  let body: any;
  b.m("GET")
  .h(headers)
  .q(query)
  .b(body);
  return b.build();
}

/**
 * serializeAws_restJson1ListLifecycleOrphansCommand
 */
export const se_ListLifecycleOrphansCommand = async(
  input: ListLifecycleOrphansCommandInput,
  context: __SerdeContext
): Promise<__HttpRequest> => {
  const b = rb(input, context);
  const headers: any = {
  };
  b.bp("/_/backbeat/lifecycle/{Bucket}");
  b.p('Bucket', () => input.Bucket!, '{Bucket}', false)
  const query: any = map({
    [_lt]: [, "orphan"],
    [_bd]: [,input[_BD]!],
    [_edsn]: [,input[_EDSN]!],
    [_et]: [,input[_ET]!],
    [_m]: [,input[_M]!],
    [_mk]: [() => input.MaxKeys !== void 0, () => (input[_MK]!.toString())],
    [_p]: [,input[_P]!],
  });
  let body: any;
  b.m("GET")
  .h(headers)
  .q(query)
  .b(body);
  return b.build();
}

/**
 * serializeAws_restJson1MultipleBackendAbortMPUCommand
 */
export const se_MultipleBackendAbortMPUCommand = async(
  input: MultipleBackendAbortMPUCommandInput,
  context: __SerdeContext
): Promise<__HttpRequest> => {
  const b = rb(input, context);
  const headers: any = map({}, isSerializableHeaderValue, {
    [_xsst]: input[_ST]!,
    [_xssc]: input[_SC]!,
    [_xsui]: input[_UI]!,
  });
  b.bp("/_/backbeat/multiplebackenddata/{Bucket}/{Key+}");
  b.p('Bucket', () => input.Bucket!, '{Bucket}', false)
  b.p('Key', () => input.Key!, '{Key+}', true)
  const query: any = map({
    [_o]: [, "abortmpu"],
  });
  let body: any;
  b.m("DELETE")
  .h(headers)
  .q(query)
  .b(body);
  return b.build();
}

/**
 * serializeAws_restJson1MultipleBackendCompleteMPUCommand
 */
export const se_MultipleBackendCompleteMPUCommand = async(
  input: MultipleBackendCompleteMPUCommandInput,
  context: __SerdeContext
): Promise<__HttpRequest> => {
  const b = rb(input, context);
  const headers: any = map({}, isSerializableHeaderValue, {
    'content-type': 'application/octet-stream',
    [_xsst]: input[_ST]!,
    [_xssc]: input[_SC]!,
    [_xsvi]: input[_VI]!,
    [_xsct]: input[_CT]!,
    [_xsum]: input[_UMD]!,
    [_xscc]: input[_CC]!,
    [_xscd]: input[_CD]!,
    [_xsce]: input[_CE]!,
    [_xsui]: input[_UI]!,
    [_xst]: input[_T]!,
  });
  b.bp("/_/backbeat/multiplebackenddata/{Bucket}/{Key+}");
  b.p('Bucket', () => input.Bucket!, '{Bucket}', false)
  b.p('Key', () => input.Key!, '{Key+}', true)
  const query: any = map({
    [_o]: [, "completempu"],
  });
  let body: any;
  if (input.Body !== undefined) {
    body = input.Body;
  }
  b.m("POST")
  .h(headers)
  .q(query)
  .b(body);
  return b.build();
}

/**
 * serializeAws_restJson1MultipleBackendDeleteObjectCommand
 */
export const se_MultipleBackendDeleteObjectCommand = async(
  input: MultipleBackendDeleteObjectCommandInput,
  context: __SerdeContext
): Promise<__HttpRequest> => {
  const b = rb(input, context);
  const headers: any = map({}, isSerializableHeaderValue, {
    [_xsst]: input[_ST]!,
    [_xssc]: input[_SC]!,
  });
  b.bp("/_/backbeat/multiplebackenddata/{Bucket}/{Key+}");
  b.p('Bucket', () => input.Bucket!, '{Bucket}', false)
  b.p('Key', () => input.Key!, '{Key+}', true)
  const query: any = map({
    [_o]: [, "deleteobject"],
  });
  let body: any;
  b.m("DELETE")
  .h(headers)
  .q(query)
  .b(body);
  return b.build();
}

/**
 * serializeAws_restJson1MultipleBackendDeleteObjectTaggingCommand
 */
export const se_MultipleBackendDeleteObjectTaggingCommand = async(
  input: MultipleBackendDeleteObjectTaggingCommandInput,
  context: __SerdeContext
): Promise<__HttpRequest> => {
  const b = rb(input, context);
  const headers: any = map({}, isSerializableHeaderValue, {
    'content-type': 'application/octet-stream',
    [_xssc]: input[_SC]!,
    [_xsst]: input[_ST]!,
    [_xsdsvi]: input[_DSVI]!,
    [_xssb]: input[_SB]!,
    [_xssvi]: input[_SVI]!,
    [_xsres]: input[_RES]!,
  });
  b.bp("/_/backbeat/multiplebackenddata/{Bucket}/{Key+}");
  b.p('Bucket', () => input.Bucket!, '{Bucket}', false)
  b.p('Key', () => input.Key!, '{Key+}', true)
  const query: any = map({
    [_o]: [, "deleteobjecttagging"],
  });
  let body: any;
  if (input.Body !== undefined) {
    body = input.Body;
  }
  b.m("DELETE")
  .h(headers)
  .q(query)
  .b(body);
  return b.build();
}

/**
 * serializeAws_restJson1MultipleBackendHeadObjectCommand
 */
export const se_MultipleBackendHeadObjectCommand = async(
  input: MultipleBackendHeadObjectCommandInput,
  context: __SerdeContext
): Promise<__HttpRequest> => {
  const b = rb(input, context);
  const headers: any = map({}, isSerializableHeaderValue, {
    [_xsl]: input[_Lo]!,
  });
  b.bp("/_/backbeat/multiplebackendmetadata/{Bucket}/{Key+}");
  b.p('Bucket', () => input.Bucket!, '{Bucket}', false)
  b.p('Key', () => input.Key!, '{Key+}', true)
  let body: any;
  b.m("GET")
  .h(headers)
  .b(body);
  return b.build();
}

/**
 * serializeAws_restJson1MultipleBackendInitiateMPUCommand
 */
export const se_MultipleBackendInitiateMPUCommand = async(
  input: MultipleBackendInitiateMPUCommandInput,
  context: __SerdeContext
): Promise<__HttpRequest> => {
  const b = rb(input, context);
  const headers: any = map({}, isSerializableHeaderValue, {
    'content-type': 'application/octet-stream',
    [_xssc]: input[_SC]!,
    [_xsvi]: input[_VI]!,
    [_xsst]: input[_ST]!,
    [_xsct]: input[_CT]!,
    [_xsum]: input[_UMD]!,
    [_xscc]: input[_CC]!,
    [_xscd]: input[_CD]!,
    [_xsce]: input[_CE]!,
    [_xst]: input[_T]!,
  });
  b.bp("/_/backbeat/multiplebackenddata/{Bucket}/{Key+}");
  b.p('Bucket', () => input.Bucket!, '{Bucket}', false)
  b.p('Key', () => input.Key!, '{Key+}', true)
  const query: any = map({
    [_o]: [, "initiatempu"],
  });
  let body: any;
  if (input.Body !== undefined) {
    body = input.Body;
  }
  b.m("POST")
  .h(headers)
  .q(query)
  .b(body);
  return b.build();
}

/**
 * serializeAws_restJson1MultipleBackendPutMPUPartCommand
 */
export const se_MultipleBackendPutMPUPartCommand = async(
  input: MultipleBackendPutMPUPartCommandInput,
  context: __SerdeContext
): Promise<__HttpRequest> => {
  const b = rb(input, context);
  const headers: any = map({}, isSerializableHeaderValue, {
    'content-type': 'application/octet-stream',
    [_xsst]: input[_ST]!,
    [_xssc]: input[_SC]!,
    [_xspn]: [() => isSerializableHeaderValue(input[_PN]), () => input[_PN]!.toString()],
    [_xsui]: input[_UI]!,
  });
  b.bp("/_/backbeat/multiplebackenddata/{Bucket}/{Key+}");
  b.p('Bucket', () => input.Bucket!, '{Bucket}', false)
  b.p('Key', () => input.Key!, '{Key+}', true)
  const query: any = map({
    [_o]: [, "putpart"],
  });
  let body: any;
  if (input.Body !== undefined) {
    body = input.Body;
  }
  b.m("PUT")
  .h(headers)
  .q(query)
  .b(body);
  return b.build();
}

/**
 * serializeAws_restJson1MultipleBackendPutObjectCommand
 */
export const se_MultipleBackendPutObjectCommand = async(
  input: MultipleBackendPutObjectCommandInput,
  context: __SerdeContext
): Promise<__HttpRequest> => {
  const b = rb(input, context);
  const headers: any = map({}, isSerializableHeaderValue, {
    'content-type': 'application/octet-stream',
    [_cm]: input[_CMD]!,
    [_xsct]: input[_CT]!,
    [_xsum]: input[_UMD]!,
    [_xscc]: input[_CC]!,
    [_xscd]: input[_CD]!,
    [_xsce]: input[_CE]!,
    [_xsci]: input[_CID]!,
    [_xssc]: input[_SC]!,
    [_xsst]: input[_ST]!,
    [_xsvi]: input[_VI]!,
    [_xst]: input[_T]!,
  });
  b.bp("/_/backbeat/multiplebackenddata/{Bucket}/{Key}");
  b.p('Bucket', () => input.Bucket!, '{Bucket}', false)
  b.p('Key', () => input.Key!, '{Key}', false)
  const query: any = map({
    [_o]: [, "putobject"],
  });
  let body: any;
  if (input.Body !== undefined) {
    body = input.Body;
  }
  b.m("PUT")
  .h(headers)
  .q(query)
  .b(body);
  return b.build();
}

/**
 * serializeAws_restJson1MultipleBackendPutObjectTaggingCommand
 */
export const se_MultipleBackendPutObjectTaggingCommand = async(
  input: MultipleBackendPutObjectTaggingCommandInput,
  context: __SerdeContext
): Promise<__HttpRequest> => {
  const b = rb(input, context);
  const headers: any = map({}, isSerializableHeaderValue, {
    'content-type': 'application/octet-stream',
    [_xsst]: input[_ST]!,
    [_xssc]: input[_SC]!,
    [_xsdsvi]: input[_DSVI]!,
    [_xst]: input[_T]!,
    [_xssb]: input[_SB]!,
    [_xssvi]: input[_SVI]!,
    [_xsres]: input[_RES]!,
  });
  b.bp("/_/backbeat/multiplebackenddata/{Bucket}/{Key+}");
  b.p('Bucket', () => input.Bucket!, '{Bucket}', false)
  b.p('Key', () => input.Key!, '{Key+}', true)
  const query: any = map({
    [_o]: [, "puttagging"],
  });
  let body: any;
  if (input.Body !== undefined) {
    body = input.Body;
  }
  b.m("POST")
  .h(headers)
  .q(query)
  .b(body);
  return b.build();
}

/**
 * serializeAws_restJson1PutBucketIndexesCommand
 */
export const se_PutBucketIndexesCommand = async(
  input: PutBucketIndexesCommandInput,
  context: __SerdeContext
): Promise<__HttpRequest> => {
  const b = rb(input, context);
  const headers: any = {
    'content-type': 'application/octet-stream',
  };
  b.bp("/_/backbeat/index/{Bucket}");
  b.p('Bucket', () => input.Bucket!, '{Bucket}', false)
  const query: any = map({
    [_o]: [, "add"],
  });
  let body: any;
  if (input.Body !== undefined) {
    body = input.Body;
  }
  b.m("POST")
  .h(headers)
  .q(query)
  .b(body);
  return b.build();
}

/**
 * serializeAws_restJson1PutDataCommand
 */
export const se_PutDataCommand = async(
  input: PutDataCommandInput,
  context: __SerdeContext
): Promise<__HttpRequest> => {
  const b = rb(input, context);
  const headers: any = map({}, isSerializableHeaderValue, {
    'content-type': 'application/octet-stream',
    [_cm]: input[_CMD]!,
    [_xsci]: input[_CID]!,
    [_xsvr]: [() => isSerializableHeaderValue(input[_VR]), () => input[_VR]!.toString()],
  });
  b.bp("/_/backbeat/data/{Bucket}/{Key}");
  b.p('Bucket', () => input.Bucket!, '{Bucket}', false)
  b.p('Key', () => input.Key!, '{Key}', false)
  const query: any = map({
    [_v]: [, ""],
  });
  let body: any;
  if (input.Body !== undefined) {
    body = input.Body;
  }
  b.m("PUT")
  .h(headers)
  .q(query)
  .b(body);
  return b.build();
}

/**
 * serializeAws_restJson1PutMetadataCommand
 */
export const se_PutMetadataCommand = async(
  input: PutMetadataCommandInput,
  context: __SerdeContext
): Promise<__HttpRequest> => {
  const b = rb(input, context);
  const headers: any = map({}, isSerializableHeaderValue, {
    'content-type': 'application/octet-stream',
    [_cm]: input[_CMD]!,
    [_xsrc]: input[_RC]!,
    [_xsvr]: [() => isSerializableHeaderValue(input[_VR]), () => input[_VR]!.toString()],
  });
  b.bp("/_/backbeat/metadata/{Bucket}/{Key+}");
  b.p('Bucket', () => input.Bucket!, '{Bucket}', false)
  b.p('Key', () => input.Key!, '{Key+}', true)
  const query: any = map({
    [_vI]: [,input[_VI]!],
    [_aI]: [,input[_AI]!],
  });
  let body: any;
  if (input.Body !== undefined) {
    body = input.Body;
  }
  b.m("PUT")
  .h(headers)
  .q(query)
  .b(body);
  return b.build();
}

/**
 * deserializeAws_restJson1BatchDeleteCommand
 */
export const de_BatchDeleteCommand = async(
  output: __HttpResponse,
  context: __SerdeContext
): Promise<BatchDeleteCommandOutput> => {
  if (output.statusCode !== 200 && output.statusCode >= 300) {
    return de_CommandError(output, context);
  }
  const contents: any = map({
    $metadata: deserializeMetadata(output),
  });
  await collectBody(output.body, context);
  return contents;
}

/**
 * deserializeAws_restJson1DeleteBucketIndexesCommand
 */
export const de_DeleteBucketIndexesCommand = async(
  output: __HttpResponse,
  context: __SerdeContext
): Promise<DeleteBucketIndexesCommandOutput> => {
  if (output.statusCode !== 200 && output.statusCode >= 300) {
    return de_CommandError(output, context);
  }
  const contents: any = map({
    $metadata: deserializeMetadata(output),
  });
  await collectBody(output.body, context);
  return contents;
}

/**
 * deserializeAws_restJson1DeleteObjectFromExpirationCommand
 */
export const de_DeleteObjectFromExpirationCommand = async(
  output: __HttpResponse,
  context: __SerdeContext
): Promise<DeleteObjectFromExpirationCommandOutput> => {
  if (output.statusCode !== 200 && output.statusCode >= 300) {
    return de_CommandError(output, context);
  }
  const contents: any = map({
    $metadata: deserializeMetadata(output),
  });
  const data: Record<string, any> = __expectNonNull((__expectObject(await parseBody(output.body, context))), "body");
  const doc = take(data, {
    'versionId': __expectString,
  });
  Object.assign(contents, doc);
  return contents;
}

/**
 * deserializeAws_restJson1GetBucketCseqCommand
 */
export const de_GetBucketCseqCommand = async(
  output: __HttpResponse,
  context: __SerdeContext
): Promise<GetBucketCseqCommandOutput> => {
  if (output.statusCode !== 200 && output.statusCode >= 300) {
    return de_CommandError(output, context);
  }
  const contents: any = map({
    $metadata: deserializeMetadata(output),
  });
  const data: Record<string, any> = __expectNonNull((__expectObject(await parseBody(output.body, context))), "body");
  const doc = take(data, {
    'CseqInfo': _json,
  });
  Object.assign(contents, doc);
  return contents;
}

/**
 * deserializeAws_restJson1GetBucketIndexesCommand
 */
export const de_GetBucketIndexesCommand = async(
  output: __HttpResponse,
  context: __SerdeContext
): Promise<GetBucketIndexesCommandOutput> => {
  if (output.statusCode !== 200 && output.statusCode >= 300) {
    return de_CommandError(output, context);
  }
  const contents: any = map({
    $metadata: deserializeMetadata(output),
  });
  const data: Record<string, any> = __expectNonNull((__expectObject(await parseBody(output.body, context))), "body");
  const doc = take(data, {
    'Indexes': _json,
  });
  Object.assign(contents, doc);
  return contents;
}

/**
 * deserializeAws_restJson1GetBucketMetadataCommand
 */
export const de_GetBucketMetadataCommand = async(
  output: __HttpResponse,
  context: __SerdeContext
): Promise<GetBucketMetadataCommandOutput> => {
  if (output.statusCode !== 200 && output.statusCode >= 300) {
    return de_CommandError(output, context);
  }
  const contents: any = map({
    $metadata: deserializeMetadata(output),
  });
  const data: Record<string, any> = __expectNonNull((__expectObject(await parseBody(output.body, context))), "body");
  const doc = take(data, {
    'acl': _ => de_Document(_, context),
    'cors': _ => de_Document(_, context),
    'creationDate': __expectString,
    'deleted': __expectBoolean,
    'lifecycleConfiguration': _ => de_Document(_, context),
    'locationConstraint': __expectString,
    'mdBucketModelVersion': __expectInt32,
    'name': __expectString,
    'owner': __expectString,
    'ownerDisplayName': __expectString,
    'readLocationConstraint': __expectString,
    'replicationConfiguration': _ => de_Document(_, context),
    'serverSideEncryption': _ => de_Document(_, context),
    'transient': __expectBoolean,
    'uid': __expectString,
    'versioningConfiguration': _ => de_Document(_, context),
  });
  Object.assign(contents, doc);
  return contents;
}

/**
 * deserializeAws_restJson1GetMetadataCommand
 */
export const de_GetMetadataCommand = async(
  output: __HttpResponse,
  context: __SerdeContext
): Promise<GetMetadataCommandOutput> => {
  if (output.statusCode !== 200 && output.statusCode >= 300) {
    return de_CommandError(output, context);
  }
  const contents: any = map({
    $metadata: deserializeMetadata(output),
  });
  const data: Record<string, any> = __expectNonNull((__expectObject(await parseBody(output.body, context))), "body");
  const doc = take(data, {
    'Body': __expectString,
  });
  Object.assign(contents, doc);
  return contents;
}

/**
 * deserializeAws_restJson1GetObjectCommand
 */
export const de_GetObjectCommand = async(
  output: __HttpResponse,
  context: __SerdeContext
): Promise<GetObjectCommandOutput> => {
  if (output.statusCode !== 200 && output.statusCode >= 300) {
    return de_CommandError(output, context);
  }
  const contents: any = map({
    $metadata: deserializeMetadata(output),
    [_CT]: [, output.headers[_ct]],
    [_ETa]: [, output.headers[_e]],
    [_LM]: [() => void 0 !== output.headers[_lm], () => __expectNonNull(__parseRfc7231DateTime(output.headers[_lm]))],
    [_VI]: [, output.headers[_xavi]],
  });
  const data: any = await collectBody(output.body, context);
  contents.Body = data;
  return contents;
}

/**
 * deserializeAws_restJson1GetObjectListCommand
 */
export const de_GetObjectListCommand = async(
  output: __HttpResponse,
  context: __SerdeContext
): Promise<GetObjectListCommandOutput> => {
  if (output.statusCode !== 200 && output.statusCode >= 300) {
    return de_CommandError(output, context);
  }
  const contents: any = map({
    $metadata: deserializeMetadata(output),
  });
  const data: Record<string, any> = __expectNonNull((__expectObject(await parseBody(output.body, context))), "body");
  const doc = take(data, {
    'CommonPrefixes': _json,
    'Contents': _json,
    'Delimiter': __expectString,
    'IsTruncated': __expectBoolean,
  });
  Object.assign(contents, doc);
  return contents;
}

/**
 * deserializeAws_restJson1GetRaftBucketsCommand
 */
export const de_GetRaftBucketsCommand = async(
  output: __HttpResponse,
  context: __SerdeContext
): Promise<GetRaftBucketsCommandOutput> => {
  if (output.statusCode !== 200 && output.statusCode >= 300) {
    return de_CommandError(output, context);
  }
  const contents: any = map({
    $metadata: deserializeMetadata(output),
  });
  const data: Record<string, any> = __expectNonNull((__expectObject(await parseBody(output.body, context))), "body");
  const doc = take(data, {
    'Buckets': _json,
  });
  Object.assign(contents, doc);
  return contents;
}

/**
 * deserializeAws_restJson1GetRaftIdCommand
 */
export const de_GetRaftIdCommand = async(
  output: __HttpResponse,
  context: __SerdeContext
): Promise<GetRaftIdCommandOutput> => {
  if (output.statusCode !== 200 && output.statusCode >= 300) {
    return de_CommandError(output, context);
  }
  const contents: any = map({
    $metadata: deserializeMetadata(output),
  });
  const data: any = await collectBodyString(output.body, context);
  contents.RaftId = __expectString(data);
  return contents;
}

/**
 * deserializeAws_restJson1GetRaftLogCommand
 */
export const de_GetRaftLogCommand = async(
  output: __HttpResponse,
  context: __SerdeContext
): Promise<GetRaftLogCommandOutput> => {
  if (output.statusCode !== 200 && output.statusCode >= 300) {
    return de_CommandError(output, context);
  }
  const contents: any = map({
    $metadata: deserializeMetadata(output),
  });
  const data: Record<string, any> = __expectNonNull((__expectObject(await parseBody(output.body, context))), "body");
  const doc = take(data, {
    'info': _json,
    'log': _json,
  });
  Object.assign(contents, doc);
  return contents;
}

/**
 * deserializeAws_restJson1ListLifecycleCurrentsCommand
 */
export const de_ListLifecycleCurrentsCommand = async(
  output: __HttpResponse,
  context: __SerdeContext
): Promise<ListLifecycleCurrentsCommandOutput> => {
  if (output.statusCode !== 200 && output.statusCode >= 300) {
    return de_CommandError(output, context);
  }
  const contents: any = map({
    $metadata: deserializeMetadata(output),
  });
  const data: Record<string, any> = __expectNonNull((__expectObject(await parseBody(output.body, context))), "body");
  const doc = take(data, {
    'BeforeDate': __expectString,
    'Contents': _json,
    'IsTruncated': __expectBoolean,
    'Marker': __expectString,
    'MaxKeys': __expectInt32,
    'Name': __expectString,
    'NextMarker': __expectString,
    'Prefix': __expectString,
  });
  Object.assign(contents, doc);
  return contents;
}

/**
 * deserializeAws_restJson1ListLifecycleNonCurrentsCommand
 */
export const de_ListLifecycleNonCurrentsCommand = async(
  output: __HttpResponse,
  context: __SerdeContext
): Promise<ListLifecycleNonCurrentsCommandOutput> => {
  if (output.statusCode !== 200 && output.statusCode >= 300) {
    return de_CommandError(output, context);
  }
  const contents: any = map({
    $metadata: deserializeMetadata(output),
  });
  const data: Record<string, any> = __expectNonNull((__expectObject(await parseBody(output.body, context))), "body");
  const doc = take(data, {
    'BeforeDate': __expectString,
    'Contents': _json,
    'IsTruncated': __expectBoolean,
    'KeyMarker': __expectString,
    'MaxKeys': __expectInt32,
    'Name': __expectString,
    'NextKeyMarker': __expectString,
    'NextVersionIdMarker': __expectString,
    'Prefix': __expectString,
    'VersionIdMarker': __expectString,
  });
  Object.assign(contents, doc);
  return contents;
}

/**
 * deserializeAws_restJson1ListLifecycleOrphansCommand
 */
export const de_ListLifecycleOrphansCommand = async(
  output: __HttpResponse,
  context: __SerdeContext
): Promise<ListLifecycleOrphansCommandOutput> => {
  if (output.statusCode !== 200 && output.statusCode >= 300) {
    return de_CommandError(output, context);
  }
  const contents: any = map({
    $metadata: deserializeMetadata(output),
  });
  const data: Record<string, any> = __expectNonNull((__expectObject(await parseBody(output.body, context))), "body");
  const doc = take(data, {
    'BeforeDate': __expectString,
    'Contents': _json,
    'IsTruncated': __expectBoolean,
    'Marker': __expectString,
    'MaxKeys': __expectInt32,
    'Name': __expectString,
    'NextMarker': __expectString,
    'Prefix': __expectString,
  });
  Object.assign(contents, doc);
  return contents;
}

/**
 * deserializeAws_restJson1MultipleBackendAbortMPUCommand
 */
export const de_MultipleBackendAbortMPUCommand = async(
  output: __HttpResponse,
  context: __SerdeContext
): Promise<MultipleBackendAbortMPUCommandOutput> => {
  if (output.statusCode !== 200 && output.statusCode >= 300) {
    return de_CommandError(output, context);
  }
  const contents: any = map({
    $metadata: deserializeMetadata(output),
  });
  await collectBody(output.body, context);
  return contents;
}

/**
 * deserializeAws_restJson1MultipleBackendCompleteMPUCommand
 */
export const de_MultipleBackendCompleteMPUCommand = async(
  output: __HttpResponse,
  context: __SerdeContext
): Promise<MultipleBackendCompleteMPUCommandOutput> => {
  if (output.statusCode !== 200 && output.statusCode >= 300) {
    return de_CommandError(output, context);
  }
  const contents: any = map({
    $metadata: deserializeMetadata(output),
  });
  const data: Record<string, any> = __expectNonNull((__expectObject(await parseBody(output.body, context))), "body");
  const doc = take(data, {
    'location': _json,
    'versionId': __expectString,
  });
  Object.assign(contents, doc);
  return contents;
}

/**
 * deserializeAws_restJson1MultipleBackendDeleteObjectCommand
 */
export const de_MultipleBackendDeleteObjectCommand = async(
  output: __HttpResponse,
  context: __SerdeContext
): Promise<MultipleBackendDeleteObjectCommandOutput> => {
  if (output.statusCode !== 200 && output.statusCode >= 300) {
    return de_CommandError(output, context);
  }
  const contents: any = map({
    $metadata: deserializeMetadata(output),
  });
  const data: Record<string, any> = __expectNonNull((__expectObject(await parseBody(output.body, context))), "body");
  const doc = take(data, {
    'versionId': __expectString,
  });
  Object.assign(contents, doc);
  return contents;
}

/**
 * deserializeAws_restJson1MultipleBackendDeleteObjectTaggingCommand
 */
export const de_MultipleBackendDeleteObjectTaggingCommand = async(
  output: __HttpResponse,
  context: __SerdeContext
): Promise<MultipleBackendDeleteObjectTaggingCommandOutput> => {
  if (output.statusCode !== 200 && output.statusCode >= 300) {
    return de_CommandError(output, context);
  }
  const contents: any = map({
    $metadata: deserializeMetadata(output),
  });
  const data: Record<string, any> = __expectNonNull((__expectObject(await parseBody(output.body, context))), "body");
  const doc = take(data, {
    'versionId': __expectString,
  });
  Object.assign(contents, doc);
  return contents;
}

/**
 * deserializeAws_restJson1MultipleBackendHeadObjectCommand
 */
export const de_MultipleBackendHeadObjectCommand = async(
  output: __HttpResponse,
  context: __SerdeContext
): Promise<MultipleBackendHeadObjectCommandOutput> => {
  if (output.statusCode !== 200 && output.statusCode >= 300) {
    return de_CommandError(output, context);
  }
  const contents: any = map({
    $metadata: deserializeMetadata(output),
  });
  const data: Record<string, any> = __expectNonNull((__expectObject(await parseBody(output.body, context))), "body");
  const doc = take(data, {
    'lastModified': __expectString,
  });
  Object.assign(contents, doc);
  return contents;
}

/**
 * deserializeAws_restJson1MultipleBackendInitiateMPUCommand
 */
export const de_MultipleBackendInitiateMPUCommand = async(
  output: __HttpResponse,
  context: __SerdeContext
): Promise<MultipleBackendInitiateMPUCommandOutput> => {
  if (output.statusCode !== 200 && output.statusCode >= 300) {
    return de_CommandError(output, context);
  }
  const contents: any = map({
    $metadata: deserializeMetadata(output),
  });
  const data: Record<string, any> = __expectNonNull((__expectObject(await parseBody(output.body, context))), "body");
  const doc = take(data, {
    'uploadId': __expectString,
  });
  Object.assign(contents, doc);
  return contents;
}

/**
 * deserializeAws_restJson1MultipleBackendPutMPUPartCommand
 */
export const de_MultipleBackendPutMPUPartCommand = async(
  output: __HttpResponse,
  context: __SerdeContext
): Promise<MultipleBackendPutMPUPartCommandOutput> => {
  if (output.statusCode !== 200 && output.statusCode >= 300) {
    return de_CommandError(output, context);
  }
  const contents: any = map({
    $metadata: deserializeMetadata(output),
  });
  const data: Record<string, any> = __expectNonNull((__expectObject(await parseBody(output.body, context))), "body");
  const doc = take(data, {
    'ETag': __expectString,
    'numberSubParts': __expectLong,
    'partNumber': __expectLong,
  });
  Object.assign(contents, doc);
  return contents;
}

/**
 * deserializeAws_restJson1MultipleBackendPutObjectCommand
 */
export const de_MultipleBackendPutObjectCommand = async(
  output: __HttpResponse,
  context: __SerdeContext
): Promise<MultipleBackendPutObjectCommandOutput> => {
  if (output.statusCode !== 200 && output.statusCode >= 300) {
    return de_CommandError(output, context);
  }
  const contents: any = map({
    $metadata: deserializeMetadata(output),
  });
  const data: Record<string, any> = __expectNonNull((__expectObject(await parseBody(output.body, context))), "body");
  const doc = take(data, {
    'location': _json,
    'versionId': __expectString,
  });
  Object.assign(contents, doc);
  return contents;
}

/**
 * deserializeAws_restJson1MultipleBackendPutObjectTaggingCommand
 */
export const de_MultipleBackendPutObjectTaggingCommand = async(
  output: __HttpResponse,
  context: __SerdeContext
): Promise<MultipleBackendPutObjectTaggingCommandOutput> => {
  if (output.statusCode !== 200 && output.statusCode >= 300) {
    return de_CommandError(output, context);
  }
  const contents: any = map({
    $metadata: deserializeMetadata(output),
  });
  const data: Record<string, any> = __expectNonNull((__expectObject(await parseBody(output.body, context))), "body");
  const doc = take(data, {
    'versionId': __expectString,
  });
  Object.assign(contents, doc);
  return contents;
}

/**
 * deserializeAws_restJson1PutBucketIndexesCommand
 */
export const de_PutBucketIndexesCommand = async(
  output: __HttpResponse,
  context: __SerdeContext
): Promise<PutBucketIndexesCommandOutput> => {
  if (output.statusCode !== 200 && output.statusCode >= 300) {
    return de_CommandError(output, context);
  }
  const contents: any = map({
    $metadata: deserializeMetadata(output),
  });
  await collectBody(output.body, context);
  return contents;
}

/**
 * deserializeAws_restJson1PutDataCommand
 */
export const de_PutDataCommand = async(
  output: __HttpResponse,
  context: __SerdeContext
): Promise<PutDataCommandOutput> => {
  if (output.statusCode !== 200 && output.statusCode >= 300) {
    return de_CommandError(output, context);
  }
  const contents: any = map({
    $metadata: deserializeMetadata(output),
  });
  const data: any = await collectBodyString(output.body, context);
  contents.LocationsData = data;
  contents.LocationsData = JSON.parse(data);
  return contents;
}

/**
 * deserializeAws_restJson1PutMetadataCommand
 */
export const de_PutMetadataCommand = async(
  output: __HttpResponse,
  context: __SerdeContext
): Promise<PutMetadataCommandOutput> => {
  if (output.statusCode !== 200 && output.statusCode >= 300) {
    return de_CommandError(output, context);
  }
  const contents: any = map({
    $metadata: deserializeMetadata(output),
  });
  const data: Record<string, any> = __expectNonNull((__expectObject(await parseBody(output.body, context))), "body");
  const doc = take(data, {
    'versionId': __expectString,
  });
  Object.assign(contents, doc);
  return contents;
}

/**
 * deserialize_Aws_restJson1CommandError
 */
const de_CommandError = async(
  output: __HttpResponse,
  context: __SerdeContext,
): Promise<never> => {
  const parsedOutput: any = {
    ...output,
    body: await parseErrorBody(output.body, context)
  };
  const errorCode = loadRestJsonErrorCode(output, parsedOutput.body);
  const parsedBody = parsedOutput.body;
  return throwDefaultError({
    output,
    parsedBody,
    errorCode
  }) as never
}

const throwDefaultError = withBaseException(__BaseException);
// se_BatchDeleteLocation omitted.

// se_BatchDeleteLocationList omitted.

// de_BucketNameList omitted.

// de_CommonPrefixList omitted.

// de_CseqInfo omitted.

// de_CseqInfoList omitted.

// de_Index omitted.

// de_IndexKey omitted.

// de_IndexKeyList omitted.

// de_IndexList omitted.

// de_LocationMDList omitted.

// de_LocationMDObj omitted.

// de_LogEntryKeyValue omitted.

// de_LogEntryList omitted.

// de_ObjectLifecycle omitted.

// de_ObjectLifecycleList omitted.

// de_ObjectMD omitted.

// de_ObjectMDList omitted.

// de_Owner omitted.

// de_RaftLogEntries omitted.

// de_RaftLogEntry omitted.

// de_RaftLogInfo omitted.

// de_Tag omitted.

// de_TagSet omitted.

/**
 * deserializeAws_restJson1Document
 */
const de_Document = (
  output: any,
  context: __SerdeContext
): __DocumentType => {
  return output;
}

const deserializeMetadata = (output: __HttpResponse): __ResponseMetadata => ({
  httpStatusCode: output.statusCode,
  requestId: output.headers["x-amzn-requestid"] ?? output.headers["x-amzn-request-id"] ?? output.headers["x-amz-request-id"],
  extendedRequestId: output.headers["x-amz-id-2"],
  cfId: output.headers["x-amz-cf-id"],
});

// Encode Uint8Array data into string with utf-8.
const collectBodyString = (streamBody: any, context: __SerdeContext): Promise<string> => collectBody(streamBody, context).then(body => context.utf8Encoder(body))

const _AI = "AccountId";
const _B = "Begin";
const _BD = "BeforeDate";
const _CC = "CacheControl";
const _CD = "ContentDisposition";
const _CE = "ContentEncoding";
const _CID = "CanonicalID";
const _CMD = "ContentMD5";
const _CT = "ContentType";
const _DSVI = "DataStoreVersionId";
const _EDSN = "ExcludedDataStoreName";
const _ET = "EncodingType";
const _ETa = "ETag";
const _IUS = "IfUnmodifiedSince";
const _KM = "KeyMarker";
const _L = "Limit";
const _LM = "LastModified";
const _Lo = "Locations";
const _M = "Marker";
const _MK = "MaxKeys";
const _P = "Prefix";
const _PN = "PartNumber";
const _RC = "ReplicationContent";
const _RES = "ReplicationEndpointSite";
const _SB = "SourceBucket";
const _SC = "StorageClass";
const _ST = "StorageType";
const _SVI = "SourceVersionId";
const _T = "Tags";
const _TL = "TargetLeader";
const _UI = "UploadId";
const _UMD = "UserMetaData";
const _VI = "VersionId";
const _VIM = "VersionIdMarker";
const _VR = "VersioningRequired";
const _aI = "accountId";
const _b = "begin";
const _bd = "before-date";
const _cm = "content-md5";
const _ct = "content-type";
const _e = "etag";
const _edsn = "excluded-data-store-name";
const _et = "encoding-type";
const _ius = "if-unmodified-since";
const _km = "key-marker";
const _l = "limit";
const _lm = "last-modified";
const _lt = "list-type";
const _m = "marker";
const _mk = "max-keys";
const _o = "operation";
const _p = "prefix";
const _tL = "targetLeader";
const _v = "v2";
const _vI = "versionId";
const _vim = "version-id-marker";
const _xavi = "x-amz-version-id";
const _xscc = "x-scal-cache-control";
const _xscd = "x-scal-content-disposition";
const _xsce = "x-scal-content-encoding";
const _xsci = "x-scal-canonical-id";
const _xsct = "x-scal-content-type";
const _xsdsvi = "x-scal-data-store-version-id";
const _xsl = "x-scal-locations";
const _xspn = "x-scal-part-number";
const _xsrc = "x-scal-replication-content";
const _xsres = "x-scal-replication-endpoint-site";
const _xssb = "x-scal-source-bucket";
const _xssc = "x-scal-storage-class";
const _xsst = "x-scal-storage-type";
const _xssvi = "x-scal-source-version-id";
const _xst = "x-scal-tags";
const _xsui = "x-scal-upload-id";
const _xsum = "x-scal-user-metadata";
const _xsvi = "x-scal-version-id";
const _xsvr = "x-scal-versioning-required";
