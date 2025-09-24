// smithy-typescript generated code
import {
  BackbeatClientResolvedConfig,
  ServiceInputTypes,
  ServiceOutputTypes,
} from "../BackbeatClient";
import { commonParams } from "../endpoint/EndpointParameters";
import {
  MultipleBackendPutObjectInput,
  MultipleBackendPutObjectOutput,
} from "../models/models_0";
import {
  de_MultipleBackendPutObjectCommand,
  se_MultipleBackendPutObjectCommand,
} from "../protocols/Aws_restJson1";
import { getEndpointPlugin } from "@smithy/middleware-endpoint";
import { getSerdePlugin } from "@smithy/middleware-serde";
import { Command as $Command } from "@smithy/smithy-client";
import {
  BlobPayloadInputTypes,
  MetadataBearer as __MetadataBearer,
} from "@smithy/types";

/**
 * @public
 */
export type { __MetadataBearer };
export { $Command };
/**
 * @public
 */
export type MultipleBackendPutObjectCommandInputType = Omit<MultipleBackendPutObjectInput, "Body"> & {
  Body?: BlobPayloadInputTypes;
};

/**
 * @public
 *
 * The input for {@link MultipleBackendPutObjectCommand}.
 */
export interface MultipleBackendPutObjectCommandInput extends MultipleBackendPutObjectCommandInputType {}
/**
 * @public
 *
 * The output of {@link MultipleBackendPutObjectCommand}.
 */
export interface MultipleBackendPutObjectCommandOutput extends MultipleBackendPutObjectOutput, __MetadataBearer {}

/**
 * @public
 *
 * @example
 * Use a bare-bones client and the command you need to make an API call.
 * ```javascript
 * import { BackbeatClient, MultipleBackendPutObjectCommand } from "@backbeat-service/client"; // ES Modules import
 * // const { BackbeatClient, MultipleBackendPutObjectCommand } = require("@backbeat-service/client"); // CommonJS import
 * const client = new BackbeatClient(config);
 * const input = { // MultipleBackendPutObjectInput
 *   Bucket: "STRING_VALUE", // required
 *   Key: "STRING_VALUE", // required
 *   ContentMD5: "STRING_VALUE",
 *   ContentType: "STRING_VALUE",
 *   UserMetaData: "STRING_VALUE",
 *   CacheControl: "STRING_VALUE",
 *   ContentDisposition: "STRING_VALUE",
 *   ContentEncoding: "STRING_VALUE",
 *   CanonicalID: "STRING_VALUE",
 *   StorageClass: "STRING_VALUE", // required
 *   StorageType: "STRING_VALUE",
 *   VersionId: "STRING_VALUE",
 *   Tags: "STRING_VALUE",
 *   Body: new Uint8Array(), // e.g. Buffer.from("") or new TextEncoder().encode("")
 * };
 * const command = new MultipleBackendPutObjectCommand(input);
 * const response = await client.send(command);
 * // { // MultipleBackendPutObjectOutput
 * //   versionId: "STRING_VALUE",
 * //   location: [ // LocationMDList
 * //     { // LocationMDObj
 * //       key: "STRING_VALUE",
 * //       size: Number("int"),
 * //       start: Number("int"),
 * //       dataStoreName: "STRING_VALUE",
 * //       dataStoreType: "STRING_VALUE",
 * //       dataStoreETag: "STRING_VALUE",
 * //       dataStoreVersionId: "STRING_VALUE",
 * //     },
 * //   ],
 * // };
 *
 * ```
 *
 * @param MultipleBackendPutObjectCommandInput - {@link MultipleBackendPutObjectCommandInput}
 * @returns {@link MultipleBackendPutObjectCommandOutput}
 * @see {@link MultipleBackendPutObjectCommandInput} for command's `input` shape.
 * @see {@link MultipleBackendPutObjectCommandOutput} for command's `response` shape.
 * @see {@link BackbeatClientResolvedConfig | config} for BackbeatClient's `config` shape.
 *
 * @throws {@link BackbeatServiceException}
 * <p>Base exception class for all service exceptions from Backbeat service.</p>
 *
 *
 */
export class MultipleBackendPutObjectCommand extends $Command.classBuilder<MultipleBackendPutObjectCommandInput, MultipleBackendPutObjectCommandOutput, BackbeatClientResolvedConfig, ServiceInputTypes, ServiceOutputTypes>()
  .ep(commonParams)
      .m(function (this: any, Command: any, cs: any, config: BackbeatClientResolvedConfig, o: any) {
          return [

  getSerdePlugin(config, this.serialize, this.deserialize),
  getEndpointPlugin(config, Command.getEndpointParameterInstructions()),
      ];
  })
  .s("Backbeat", "MultipleBackendPutObject", {

  })
  .n("BackbeatClient", "MultipleBackendPutObjectCommand")
  .f(void 0, void 0)
  .ser(se_MultipleBackendPutObjectCommand)
  .de(de_MultipleBackendPutObjectCommand)
.build() {
/** @internal type navigation helper, not in runtime. */
declare protected static __types: {
  api: {
      input: MultipleBackendPutObjectInput;
      output: MultipleBackendPutObjectOutput;
  };
  sdk: {
      input: MultipleBackendPutObjectCommandInput;
      output: MultipleBackendPutObjectCommandOutput;
  };
};
}
