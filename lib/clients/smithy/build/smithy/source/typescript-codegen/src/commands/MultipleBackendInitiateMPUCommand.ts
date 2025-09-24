// smithy-typescript generated code
import {
  BackbeatClientResolvedConfig,
  ServiceInputTypes,
  ServiceOutputTypes,
} from "../BackbeatClient";
import { commonParams } from "../endpoint/EndpointParameters";
import {
  MultipleBackendInitiateMPUInput,
  MultipleBackendInitiateMPUOutput,
} from "../models/models_0";
import {
  de_MultipleBackendInitiateMPUCommand,
  se_MultipleBackendInitiateMPUCommand,
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
export type MultipleBackendInitiateMPUCommandInputType = Omit<MultipleBackendInitiateMPUInput, "Body"> & {
  Body?: BlobPayloadInputTypes;
};

/**
 * @public
 *
 * The input for {@link MultipleBackendInitiateMPUCommand}.
 */
export interface MultipleBackendInitiateMPUCommandInput extends MultipleBackendInitiateMPUCommandInputType {}
/**
 * @public
 *
 * The output of {@link MultipleBackendInitiateMPUCommand}.
 */
export interface MultipleBackendInitiateMPUCommandOutput extends MultipleBackendInitiateMPUOutput, __MetadataBearer {}

/**
 * Initiates a multipart upload for multiple backend storage
 * @example
 * Use a bare-bones client and the command you need to make an API call.
 * ```javascript
 * import { BackbeatClient, MultipleBackendInitiateMPUCommand } from "@backbeat-service/client"; // ES Modules import
 * // const { BackbeatClient, MultipleBackendInitiateMPUCommand } = require("@backbeat-service/client"); // CommonJS import
 * const client = new BackbeatClient(config);
 * const input = { // MultipleBackendInitiateMPUInput
 *   Bucket: "STRING_VALUE", // required
 *   Key: "STRING_VALUE", // required
 *   StorageClass: "STRING_VALUE", // required
 *   VersionId: "STRING_VALUE",
 *   StorageType: "STRING_VALUE",
 *   ContentType: "STRING_VALUE",
 *   UserMetaData: "STRING_VALUE",
 *   CacheControl: "STRING_VALUE",
 *   ContentDisposition: "STRING_VALUE",
 *   ContentEncoding: "STRING_VALUE",
 *   Tags: "STRING_VALUE",
 *   Body: new Uint8Array(), // e.g. Buffer.from("") or new TextEncoder().encode("")
 * };
 * const command = new MultipleBackendInitiateMPUCommand(input);
 * const response = await client.send(command);
 * // { // MultipleBackendInitiateMPUOutput
 * //   uploadId: "STRING_VALUE",
 * // };
 *
 * ```
 *
 * @param MultipleBackendInitiateMPUCommandInput - {@link MultipleBackendInitiateMPUCommandInput}
 * @returns {@link MultipleBackendInitiateMPUCommandOutput}
 * @see {@link MultipleBackendInitiateMPUCommandInput} for command's `input` shape.
 * @see {@link MultipleBackendInitiateMPUCommandOutput} for command's `response` shape.
 * @see {@link BackbeatClientResolvedConfig | config} for BackbeatClient's `config` shape.
 *
 * @throws {@link BackbeatServiceException}
 * <p>Base exception class for all service exceptions from Backbeat service.</p>
 *
 *
 * @public
 */
export class MultipleBackendInitiateMPUCommand extends $Command.classBuilder<MultipleBackendInitiateMPUCommandInput, MultipleBackendInitiateMPUCommandOutput, BackbeatClientResolvedConfig, ServiceInputTypes, ServiceOutputTypes>()
  .ep(commonParams)
      .m(function (this: any, Command: any, cs: any, config: BackbeatClientResolvedConfig, o: any) {
          return [

  getSerdePlugin(config, this.serialize, this.deserialize),
  getEndpointPlugin(config, Command.getEndpointParameterInstructions()),
      ];
  })
  .s("Backbeat", "MultipleBackendInitiateMPU", {

  })
  .n("BackbeatClient", "MultipleBackendInitiateMPUCommand")
  .f(void 0, void 0)
  .ser(se_MultipleBackendInitiateMPUCommand)
  .de(de_MultipleBackendInitiateMPUCommand)
.build() {
/** @internal type navigation helper, not in runtime. */
declare protected static __types: {
  api: {
      input: MultipleBackendInitiateMPUInput;
      output: MultipleBackendInitiateMPUOutput;
  };
  sdk: {
      input: MultipleBackendInitiateMPUCommandInput;
      output: MultipleBackendInitiateMPUCommandOutput;
  };
};
}
