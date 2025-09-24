// smithy-typescript generated code
import {
  BackbeatClientResolvedConfig,
  ServiceInputTypes,
  ServiceOutputTypes,
} from "../BackbeatClient";
import { commonParams } from "../endpoint/EndpointParameters";
import {
  MultipleBackendHeadObjectInput,
  MultipleBackendHeadObjectOutput,
} from "../models/models_0";
import {
  de_MultipleBackendHeadObjectCommand,
  se_MultipleBackendHeadObjectCommand,
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
 * The input for {@link MultipleBackendHeadObjectCommand}.
 */
export interface MultipleBackendHeadObjectCommandInput extends MultipleBackendHeadObjectInput {}
/**
 * @public
 *
 * The output of {@link MultipleBackendHeadObjectCommand}.
 */
export interface MultipleBackendHeadObjectCommandOutput extends MultipleBackendHeadObjectOutput, __MetadataBearer {}

/**
 * Retrieves metadata for an object from multiple backend storage
 * @example
 * Use a bare-bones client and the command you need to make an API call.
 * ```javascript
 * import { BackbeatClient, MultipleBackendHeadObjectCommand } from "@backbeat-service/client"; // ES Modules import
 * // const { BackbeatClient, MultipleBackendHeadObjectCommand } = require("@backbeat-service/client"); // CommonJS import
 * const client = new BackbeatClient(config);
 * const input = { // MultipleBackendHeadObjectInput
 *   Bucket: "STRING_VALUE", // required
 *   Key: "STRING_VALUE", // required
 *   Locations: "STRING_VALUE", // required
 * };
 * const command = new MultipleBackendHeadObjectCommand(input);
 * const response = await client.send(command);
 * // { // MultipleBackendHeadObjectOutput
 * //   lastModified: "STRING_VALUE",
 * // };
 *
 * ```
 *
 * @param MultipleBackendHeadObjectCommandInput - {@link MultipleBackendHeadObjectCommandInput}
 * @returns {@link MultipleBackendHeadObjectCommandOutput}
 * @see {@link MultipleBackendHeadObjectCommandInput} for command's `input` shape.
 * @see {@link MultipleBackendHeadObjectCommandOutput} for command's `response` shape.
 * @see {@link BackbeatClientResolvedConfig | config} for BackbeatClient's `config` shape.
 *
 * @throws {@link BackbeatServiceException}
 * <p>Base exception class for all service exceptions from Backbeat service.</p>
 *
 *
 * @public
 */
export class MultipleBackendHeadObjectCommand extends $Command.classBuilder<MultipleBackendHeadObjectCommandInput, MultipleBackendHeadObjectCommandOutput, BackbeatClientResolvedConfig, ServiceInputTypes, ServiceOutputTypes>()
  .ep(commonParams)
      .m(function (this: any, Command: any, cs: any, config: BackbeatClientResolvedConfig, o: any) {
          return [

  getSerdePlugin(config, this.serialize, this.deserialize),
  getEndpointPlugin(config, Command.getEndpointParameterInstructions()),
      ];
  })
  .s("Backbeat", "MultipleBackendHeadObject", {

  })
  .n("BackbeatClient", "MultipleBackendHeadObjectCommand")
  .f(void 0, void 0)
  .ser(se_MultipleBackendHeadObjectCommand)
  .de(de_MultipleBackendHeadObjectCommand)
.build() {
/** @internal type navigation helper, not in runtime. */
declare protected static __types: {
  api: {
      input: MultipleBackendHeadObjectInput;
      output: MultipleBackendHeadObjectOutput;
  };
  sdk: {
      input: MultipleBackendHeadObjectCommandInput;
      output: MultipleBackendHeadObjectCommandOutput;
  };
};
}
