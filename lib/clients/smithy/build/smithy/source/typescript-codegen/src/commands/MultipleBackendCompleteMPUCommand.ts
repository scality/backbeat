// smithy-typescript generated code
import {
  BackbeatClientResolvedConfig,
  ServiceInputTypes,
  ServiceOutputTypes,
} from "../BackbeatClient";
import { commonParams } from "../endpoint/EndpointParameters";
import {
  MultipleBackendCompleteMPUInput,
  MultipleBackendCompleteMPUOutput,
} from "../models/models_0";
import {
  de_MultipleBackendCompleteMPUCommand,
  se_MultipleBackendCompleteMPUCommand,
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
export type MultipleBackendCompleteMPUCommandInputType = Omit<MultipleBackendCompleteMPUInput, "Body"> & {
  Body?: BlobPayloadInputTypes;
};

/**
 * @public
 *
 * The input for {@link MultipleBackendCompleteMPUCommand}.
 */
export interface MultipleBackendCompleteMPUCommandInput extends MultipleBackendCompleteMPUCommandInputType {}
/**
 * @public
 *
 * The output of {@link MultipleBackendCompleteMPUCommand}.
 */
export interface MultipleBackendCompleteMPUCommandOutput extends MultipleBackendCompleteMPUOutput, __MetadataBearer {}

/**
 * Completes a multipart upload for multiple backend storage
 * @example
 * Use a bare-bones client and the command you need to make an API call.
 * ```javascript
 * import { BackbeatClient, MultipleBackendCompleteMPUCommand } from "@backbeat-service/client"; // ES Modules import
 * // const { BackbeatClient, MultipleBackendCompleteMPUCommand } = require("@backbeat-service/client"); // CommonJS import
 * const client = new BackbeatClient(config);
 * const input = { // MultipleBackendCompleteMPUInput
 *   Bucket: "STRING_VALUE", // required
 *   Key: "STRING_VALUE", // required
 *   StorageType: "STRING_VALUE",
 *   StorageClass: "STRING_VALUE", // required
 *   VersionId: "STRING_VALUE",
 *   ContentType: "STRING_VALUE",
 *   UserMetaData: "STRING_VALUE",
 *   CacheControl: "STRING_VALUE",
 *   ContentDisposition: "STRING_VALUE",
 *   ContentEncoding: "STRING_VALUE",
 *   UploadId: "STRING_VALUE",
 *   Tags: "STRING_VALUE",
 *   Body: new Uint8Array(), // e.g. Buffer.from("") or new TextEncoder().encode("")
 * };
 * const command = new MultipleBackendCompleteMPUCommand(input);
 * const response = await client.send(command);
 * // { // MultipleBackendCompleteMPUOutput
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
 * @param MultipleBackendCompleteMPUCommandInput - {@link MultipleBackendCompleteMPUCommandInput}
 * @returns {@link MultipleBackendCompleteMPUCommandOutput}
 * @see {@link MultipleBackendCompleteMPUCommandInput} for command's `input` shape.
 * @see {@link MultipleBackendCompleteMPUCommandOutput} for command's `response` shape.
 * @see {@link BackbeatClientResolvedConfig | config} for BackbeatClient's `config` shape.
 *
 * @throws {@link BackbeatServiceException}
 * <p>Base exception class for all service exceptions from Backbeat service.</p>
 *
 *
 * @public
 */
export class MultipleBackendCompleteMPUCommand extends $Command.classBuilder<MultipleBackendCompleteMPUCommandInput, MultipleBackendCompleteMPUCommandOutput, BackbeatClientResolvedConfig, ServiceInputTypes, ServiceOutputTypes>()
  .ep(commonParams)
      .m(function (this: any, Command: any, cs: any, config: BackbeatClientResolvedConfig, o: any) {
          return [

  getSerdePlugin(config, this.serialize, this.deserialize),
  getEndpointPlugin(config, Command.getEndpointParameterInstructions()),
      ];
  })
  .s("Backbeat", "MultipleBackendCompleteMPU", {

  })
  .n("BackbeatClient", "MultipleBackendCompleteMPUCommand")
  .f(void 0, void 0)
  .ser(se_MultipleBackendCompleteMPUCommand)
  .de(de_MultipleBackendCompleteMPUCommand)
.build() {
/** @internal type navigation helper, not in runtime. */
declare protected static __types: {
  api: {
      input: MultipleBackendCompleteMPUInput;
      output: MultipleBackendCompleteMPUOutput;
  };
  sdk: {
      input: MultipleBackendCompleteMPUCommandInput;
      output: MultipleBackendCompleteMPUCommandOutput;
  };
};
}
