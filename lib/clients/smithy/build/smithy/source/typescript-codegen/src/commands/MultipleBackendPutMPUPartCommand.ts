// smithy-typescript generated code
import {
  BackbeatClientResolvedConfig,
  ServiceInputTypes,
  ServiceOutputTypes,
} from "../BackbeatClient";
import { commonParams } from "../endpoint/EndpointParameters";
import {
  MultipleBackendPutMPUPartInput,
  MultipleBackendPutMPUPartOutput,
} from "../models/models_0";
import {
  de_MultipleBackendPutMPUPartCommand,
  se_MultipleBackendPutMPUPartCommand,
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
export type MultipleBackendPutMPUPartCommandInputType = Omit<MultipleBackendPutMPUPartInput, "Body"> & {
  Body?: BlobPayloadInputTypes;
};

/**
 * @public
 *
 * The input for {@link MultipleBackendPutMPUPartCommand}.
 */
export interface MultipleBackendPutMPUPartCommandInput extends MultipleBackendPutMPUPartCommandInputType {}
/**
 * @public
 *
 * The output of {@link MultipleBackendPutMPUPartCommand}.
 */
export interface MultipleBackendPutMPUPartCommandOutput extends MultipleBackendPutMPUPartOutput, __MetadataBearer {}

/**
 * Uploads a part for a multipart upload to multiple backend storage
 * @example
 * Use a bare-bones client and the command you need to make an API call.
 * ```javascript
 * import { BackbeatClient, MultipleBackendPutMPUPartCommand } from "@backbeat-service/client"; // ES Modules import
 * // const { BackbeatClient, MultipleBackendPutMPUPartCommand } = require("@backbeat-service/client"); // CommonJS import
 * const client = new BackbeatClient(config);
 * const input = { // MultipleBackendPutMPUPartInput
 *   Bucket: "STRING_VALUE", // required
 *   Key: "STRING_VALUE", // required
 *   StorageType: "STRING_VALUE",
 *   StorageClass: "STRING_VALUE", // required
 *   PartNumber: Number("long"),
 *   UploadId: "STRING_VALUE",
 *   Body: new Uint8Array(), // e.g. Buffer.from("") or new TextEncoder().encode("")
 * };
 * const command = new MultipleBackendPutMPUPartCommand(input);
 * const response = await client.send(command);
 * // { // MultipleBackendPutMPUPartOutput
 * //   partNumber: Number("long"),
 * //   ETag: "STRING_VALUE",
 * //   numberSubParts: Number("long"),
 * // };
 *
 * ```
 *
 * @param MultipleBackendPutMPUPartCommandInput - {@link MultipleBackendPutMPUPartCommandInput}
 * @returns {@link MultipleBackendPutMPUPartCommandOutput}
 * @see {@link MultipleBackendPutMPUPartCommandInput} for command's `input` shape.
 * @see {@link MultipleBackendPutMPUPartCommandOutput} for command's `response` shape.
 * @see {@link BackbeatClientResolvedConfig | config} for BackbeatClient's `config` shape.
 *
 * @throws {@link BackbeatServiceException}
 * <p>Base exception class for all service exceptions from Backbeat service.</p>
 *
 *
 * @public
 */
export class MultipleBackendPutMPUPartCommand extends $Command.classBuilder<MultipleBackendPutMPUPartCommandInput, MultipleBackendPutMPUPartCommandOutput, BackbeatClientResolvedConfig, ServiceInputTypes, ServiceOutputTypes>()
  .ep(commonParams)
      .m(function (this: any, Command: any, cs: any, config: BackbeatClientResolvedConfig, o: any) {
          return [

  getSerdePlugin(config, this.serialize, this.deserialize),
  getEndpointPlugin(config, Command.getEndpointParameterInstructions()),
      ];
  })
  .s("Backbeat", "MultipleBackendPutMPUPart", {

  })
  .n("BackbeatClient", "MultipleBackendPutMPUPartCommand")
  .f(void 0, void 0)
  .ser(se_MultipleBackendPutMPUPartCommand)
  .de(de_MultipleBackendPutMPUPartCommand)
.build() {
/** @internal type navigation helper, not in runtime. */
declare protected static __types: {
  api: {
      input: MultipleBackendPutMPUPartInput;
      output: MultipleBackendPutMPUPartOutput;
  };
  sdk: {
      input: MultipleBackendPutMPUPartCommandInput;
      output: MultipleBackendPutMPUPartCommandOutput;
  };
};
}
