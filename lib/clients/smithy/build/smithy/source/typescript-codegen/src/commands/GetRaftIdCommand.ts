// smithy-typescript generated code
import {
  BackbeatClientResolvedConfig,
  ServiceInputTypes,
  ServiceOutputTypes,
} from "../BackbeatClient";
import { commonParams } from "../endpoint/EndpointParameters";
import {
  GetRaftIdInput,
  GetRaftIdOutput,
} from "../models/models_0";
import {
  de_GetRaftIdCommand,
  se_GetRaftIdCommand,
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
 * The input for {@link GetRaftIdCommand}.
 */
export interface GetRaftIdCommandInput extends GetRaftIdInput {}
/**
 * @public
 *
 * The output of {@link GetRaftIdCommand}.
 */
export interface GetRaftIdCommandOutput extends GetRaftIdOutput, __MetadataBearer {}

/**
 * @public
 *
 * @example
 * Use a bare-bones client and the command you need to make an API call.
 * ```javascript
 * import { BackbeatClient, GetRaftIdCommand } from "@backbeat-service/client"; // ES Modules import
 * // const { BackbeatClient, GetRaftIdCommand } = require("@backbeat-service/client"); // CommonJS import
 * const client = new BackbeatClient(config);
 * const input = { // GetRaftIdInput
 *   Bucket: "STRING_VALUE", // required
 * };
 * const command = new GetRaftIdCommand(input);
 * const response = await client.send(command);
 * // { // GetRaftIdOutput
 * //   RaftId: "STRING_VALUE",
 * // };
 *
 * ```
 *
 * @param GetRaftIdCommandInput - {@link GetRaftIdCommandInput}
 * @returns {@link GetRaftIdCommandOutput}
 * @see {@link GetRaftIdCommandInput} for command's `input` shape.
 * @see {@link GetRaftIdCommandOutput} for command's `response` shape.
 * @see {@link BackbeatClientResolvedConfig | config} for BackbeatClient's `config` shape.
 *
 * @throws {@link BackbeatServiceException}
 * <p>Base exception class for all service exceptions from Backbeat service.</p>
 *
 *
 */
export class GetRaftIdCommand extends $Command.classBuilder<GetRaftIdCommandInput, GetRaftIdCommandOutput, BackbeatClientResolvedConfig, ServiceInputTypes, ServiceOutputTypes>()
  .ep(commonParams)
      .m(function (this: any, Command: any, cs: any, config: BackbeatClientResolvedConfig, o: any) {
          return [

  getSerdePlugin(config, this.serialize, this.deserialize),
  getEndpointPlugin(config, Command.getEndpointParameterInstructions()),
      ];
  })
  .s("Backbeat", "GetRaftId", {

  })
  .n("BackbeatClient", "GetRaftIdCommand")
  .f(void 0, void 0)
  .ser(se_GetRaftIdCommand)
  .de(de_GetRaftIdCommand)
.build() {
/** @internal type navigation helper, not in runtime. */
declare protected static __types: {
  api: {
      input: GetRaftIdInput;
      output: GetRaftIdOutput;
  };
  sdk: {
      input: GetRaftIdCommandInput;
      output: GetRaftIdCommandOutput;
  };
};
}
