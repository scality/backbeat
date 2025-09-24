// smithy-typescript generated code
import {
  BackbeatClientResolvedConfig,
  ServiceInputTypes,
  ServiceOutputTypes,
} from "../BackbeatClient";
import { commonParams } from "../endpoint/EndpointParameters";
import {
  MultipleBackendAbortMPUInput,
  MultipleBackendAbortMPUOutput,
} from "../models/models_0";
import {
  de_MultipleBackendAbortMPUCommand,
  se_MultipleBackendAbortMPUCommand,
} from "../protocols/Aws_restJson1";
import { getEndpointPlugin } from "@smithy/middleware-endpoint";
import { getSerdePlugin } from "@smithy/middleware-serde";
import { Command as $Command } from "@smithy/smithy-client";
import { MetadataBearer as __MetadataBearer } from "@smithy/types";

/**
 * @public
 */
export type { __MetadataBearer };
export { $Command };
/**
 * @public
 *
 * The input for {@link MultipleBackendAbortMPUCommand}.
 */
export interface MultipleBackendAbortMPUCommandInput extends MultipleBackendAbortMPUInput {}
/**
 * @public
 *
 * The output of {@link MultipleBackendAbortMPUCommand}.
 */
export interface MultipleBackendAbortMPUCommandOutput extends MultipleBackendAbortMPUOutput, __MetadataBearer {}

/**
 * Aborts a multipart upload for multiple backend storage
 * @example
 * Use a bare-bones client and the command you need to make an API call.
 * ```javascript
 * import { BackbeatClient, MultipleBackendAbortMPUCommand } from "@backbeat-service/client"; // ES Modules import
 * // const { BackbeatClient, MultipleBackendAbortMPUCommand } = require("@backbeat-service/client"); // CommonJS import
 * const client = new BackbeatClient(config);
 * const input = { // MultipleBackendAbortMPUInput
 *   Bucket: "STRING_VALUE", // required
 *   Key: "STRING_VALUE", // required
 *   StorageType: "STRING_VALUE",
 *   StorageClass: "STRING_VALUE", // required
 *   UploadId: "STRING_VALUE",
 * };
 * const command = new MultipleBackendAbortMPUCommand(input);
 * const response = await client.send(command);
 * // {};
 *
 * ```
 *
 * @param MultipleBackendAbortMPUCommandInput - {@link MultipleBackendAbortMPUCommandInput}
 * @returns {@link MultipleBackendAbortMPUCommandOutput}
 * @see {@link MultipleBackendAbortMPUCommandInput} for command's `input` shape.
 * @see {@link MultipleBackendAbortMPUCommandOutput} for command's `response` shape.
 * @see {@link BackbeatClientResolvedConfig | config} for BackbeatClient's `config` shape.
 *
 * @throws {@link BackbeatServiceException}
 * <p>Base exception class for all service exceptions from Backbeat service.</p>
 *
 *
 * @public
 */
export class MultipleBackendAbortMPUCommand extends $Command.classBuilder<MultipleBackendAbortMPUCommandInput, MultipleBackendAbortMPUCommandOutput, BackbeatClientResolvedConfig, ServiceInputTypes, ServiceOutputTypes>()
  .ep(commonParams)
      .m(function (this: any, Command: any, cs: any, config: BackbeatClientResolvedConfig, o: any) {
          return [

  getSerdePlugin(config, this.serialize, this.deserialize),
  getEndpointPlugin(config, Command.getEndpointParameterInstructions()),
      ];
  })
  .s("Backbeat", "MultipleBackendAbortMPU", {

  })
  .n("BackbeatClient", "MultipleBackendAbortMPUCommand")
  .f(void 0, void 0)
  .ser(se_MultipleBackendAbortMPUCommand)
  .de(de_MultipleBackendAbortMPUCommand)
.build() {
/** @internal type navigation helper, not in runtime. */
declare protected static __types: {
  api: {
      input: MultipleBackendAbortMPUInput;
      output: {};
  };
  sdk: {
      input: MultipleBackendAbortMPUCommandInput;
      output: MultipleBackendAbortMPUCommandOutput;
  };
};
}
