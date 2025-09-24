// smithy-typescript generated code
import {
  BackbeatClientResolvedConfig,
  ServiceInputTypes,
  ServiceOutputTypes,
} from "../BackbeatClient";
import { commonParams } from "../endpoint/EndpointParameters";
import {
  BatchDeleteInput,
  BatchDeleteOutput,
} from "../models/models_0";
import {
  de_BatchDeleteCommand,
  se_BatchDeleteCommand,
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
 * The input for {@link BatchDeleteCommand}.
 */
export interface BatchDeleteCommandInput extends BatchDeleteInput {}
/**
 * @public
 *
 * The output of {@link BatchDeleteCommand}.
 */
export interface BatchDeleteCommandOutput extends BatchDeleteOutput, __MetadataBearer {}

/**
 * @public
 *
 * @example
 * Use a bare-bones client and the command you need to make an API call.
 * ```javascript
 * import { BackbeatClient, BatchDeleteCommand } from "@backbeat-service/client"; // ES Modules import
 * // const { BackbeatClient, BatchDeleteCommand } = require("@backbeat-service/client"); // CommonJS import
 * const client = new BackbeatClient(config);
 * const input = { // BatchDeleteInput
 *   Bucket: "STRING_VALUE", // required
 *   Key: "STRING_VALUE", // required
 *   IfUnmodifiedSince: "STRING_VALUE",
 *   StorageClass: "STRING_VALUE",
 *   Tags: "STRING_VALUE",
 *   ContentType: "STRING_VALUE",
 *   Locations: [ // BatchDeleteLocationList
 *     { // BatchDeleteLocation
 *       dataStoreName: "STRING_VALUE", // required
 *       key: "STRING_VALUE", // required
 *       size: Number("int"),
 *       dataStoreVersionId: "STRING_VALUE",
 *     },
 *   ],
 * };
 * const command = new BatchDeleteCommand(input);
 * const response = await client.send(command);
 * // {};
 *
 * ```
 *
 * @param BatchDeleteCommandInput - {@link BatchDeleteCommandInput}
 * @returns {@link BatchDeleteCommandOutput}
 * @see {@link BatchDeleteCommandInput} for command's `input` shape.
 * @see {@link BatchDeleteCommandOutput} for command's `response` shape.
 * @see {@link BackbeatClientResolvedConfig | config} for BackbeatClient's `config` shape.
 *
 * @throws {@link BackbeatServiceException}
 * <p>Base exception class for all service exceptions from Backbeat service.</p>
 *
 *
 */
export class BatchDeleteCommand extends $Command.classBuilder<BatchDeleteCommandInput, BatchDeleteCommandOutput, BackbeatClientResolvedConfig, ServiceInputTypes, ServiceOutputTypes>()
  .ep(commonParams)
      .m(function (this: any, Command: any, cs: any, config: BackbeatClientResolvedConfig, o: any) {
          return [

  getSerdePlugin(config, this.serialize, this.deserialize),
  getEndpointPlugin(config, Command.getEndpointParameterInstructions()),
      ];
  })
  .s("Backbeat", "BatchDelete", {

  })
  .n("BackbeatClient", "BatchDeleteCommand")
  .f(void 0, void 0)
  .ser(se_BatchDeleteCommand)
  .de(de_BatchDeleteCommand)
.build() {
/** @internal type navigation helper, not in runtime. */
declare protected static __types: {
  api: {
      input: BatchDeleteInput;
      output: {};
  };
  sdk: {
      input: BatchDeleteCommandInput;
      output: BatchDeleteCommandOutput;
  };
};
}
