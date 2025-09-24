// smithy-typescript generated code
import {
  BackbeatClientResolvedConfig,
  ServiceInputTypes,
  ServiceOutputTypes,
} from "../BackbeatClient";
import { commonParams } from "../endpoint/EndpointParameters";
import {
  MultipleBackendDeleteObjectInput,
  MultipleBackendDeleteObjectOutput,
} from "../models/models_0";
import {
  de_MultipleBackendDeleteObjectCommand,
  se_MultipleBackendDeleteObjectCommand,
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
 * The input for {@link MultipleBackendDeleteObjectCommand}.
 */
export interface MultipleBackendDeleteObjectCommandInput extends MultipleBackendDeleteObjectInput {}
/**
 * @public
 *
 * The output of {@link MultipleBackendDeleteObjectCommand}.
 */
export interface MultipleBackendDeleteObjectCommandOutput extends MultipleBackendDeleteObjectOutput, __MetadataBearer {}

/**
 * @public
 *
 * @example
 * Use a bare-bones client and the command you need to make an API call.
 * ```javascript
 * import { BackbeatClient, MultipleBackendDeleteObjectCommand } from "@backbeat-service/client"; // ES Modules import
 * // const { BackbeatClient, MultipleBackendDeleteObjectCommand } = require("@backbeat-service/client"); // CommonJS import
 * const client = new BackbeatClient(config);
 * const input = { // MultipleBackendDeleteObjectInput
 *   Bucket: "STRING_VALUE", // required
 *   Key: "STRING_VALUE", // required
 *   StorageType: "STRING_VALUE",
 *   StorageClass: "STRING_VALUE", // required
 * };
 * const command = new MultipleBackendDeleteObjectCommand(input);
 * const response = await client.send(command);
 * // { // MultipleBackendDeleteObjectOutput
 * //   versionId: "STRING_VALUE",
 * // };
 *
 * ```
 *
 * @param MultipleBackendDeleteObjectCommandInput - {@link MultipleBackendDeleteObjectCommandInput}
 * @returns {@link MultipleBackendDeleteObjectCommandOutput}
 * @see {@link MultipleBackendDeleteObjectCommandInput} for command's `input` shape.
 * @see {@link MultipleBackendDeleteObjectCommandOutput} for command's `response` shape.
 * @see {@link BackbeatClientResolvedConfig | config} for BackbeatClient's `config` shape.
 *
 * @throws {@link BackbeatServiceException}
 * <p>Base exception class for all service exceptions from Backbeat service.</p>
 *
 *
 */
export class MultipleBackendDeleteObjectCommand extends $Command.classBuilder<MultipleBackendDeleteObjectCommandInput, MultipleBackendDeleteObjectCommandOutput, BackbeatClientResolvedConfig, ServiceInputTypes, ServiceOutputTypes>()
  .ep(commonParams)
      .m(function (this: any, Command: any, cs: any, config: BackbeatClientResolvedConfig, o: any) {
          return [

  getSerdePlugin(config, this.serialize, this.deserialize),
  getEndpointPlugin(config, Command.getEndpointParameterInstructions()),
      ];
  })
  .s("Backbeat", "MultipleBackendDeleteObject", {

  })
  .n("BackbeatClient", "MultipleBackendDeleteObjectCommand")
  .f(void 0, void 0)
  .ser(se_MultipleBackendDeleteObjectCommand)
  .de(de_MultipleBackendDeleteObjectCommand)
.build() {
/** @internal type navigation helper, not in runtime. */
declare protected static __types: {
  api: {
      input: MultipleBackendDeleteObjectInput;
      output: MultipleBackendDeleteObjectOutput;
  };
  sdk: {
      input: MultipleBackendDeleteObjectCommandInput;
      output: MultipleBackendDeleteObjectCommandOutput;
  };
};
}
