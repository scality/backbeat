// smithy-typescript generated code
import {
  BackbeatClientResolvedConfig,
  ServiceInputTypes,
  ServiceOutputTypes,
} from "../BackbeatClient";
import { commonParams } from "../endpoint/EndpointParameters";
import {
  ListLifecycleCurrentsInput,
  ListLifecycleCurrentsOutput,
} from "../models/models_0";
import {
  de_ListLifecycleCurrentsCommand,
  se_ListLifecycleCurrentsCommand,
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
 * The input for {@link ListLifecycleCurrentsCommand}.
 */
export interface ListLifecycleCurrentsCommandInput extends ListLifecycleCurrentsInput {}
/**
 * @public
 *
 * The output of {@link ListLifecycleCurrentsCommand}.
 */
export interface ListLifecycleCurrentsCommandOutput extends ListLifecycleCurrentsOutput, __MetadataBearer {}

/**
 * List lifecycle current objects operation
 * @example
 * Use a bare-bones client and the command you need to make an API call.
 * ```javascript
 * import { BackbeatClient, ListLifecycleCurrentsCommand } from "@backbeat-service/client"; // ES Modules import
 * // const { BackbeatClient, ListLifecycleCurrentsCommand } = require("@backbeat-service/client"); // CommonJS import
 * const client = new BackbeatClient(config);
 * const input = { // ListLifecycleCurrentsInput
 *   Bucket: "STRING_VALUE", // required
 *   BeforeDate: "STRING_VALUE",
 *   ExcludedDataStoreName: "STRING_VALUE",
 *   EncodingType: "STRING_VALUE",
 *   Marker: "STRING_VALUE",
 *   MaxKeys: Number("int"),
 *   Prefix: "STRING_VALUE",
 * };
 * const command = new ListLifecycleCurrentsCommand(input);
 * const response = await client.send(command);
 * // { // ListLifecycleCurrentsOutput
 * //   BeforeDate: "STRING_VALUE",
 * //   Marker: "STRING_VALUE",
 * //   IsTruncated: true || false,
 * //   NextMarker: "STRING_VALUE",
 * //   Contents: [ // ObjectLifecycleList
 * //     { // ObjectLifecycle
 * //       Key: "STRING_VALUE",
 * //       LastModified: "STRING_VALUE",
 * //       ETag: "STRING_VALUE",
 * //       Owner: { // Owner
 * //         DisplayName: "STRING_VALUE",
 * //         ID: "STRING_VALUE",
 * //       },
 * //       Size: Number("int"),
 * //       StorageClass: "STRING_VALUE",
 * //       TagSet: [ // TagSet
 * //         { // Tag
 * //           Key: "STRING_VALUE", // required
 * //           Value: "STRING_VALUE", // required
 * //         },
 * //       ],
 * //       staleDate: "STRING_VALUE",
 * //       VersionId: "STRING_VALUE",
 * //       DataStoreName: "STRING_VALUE",
 * //       ListType: "STRING_VALUE",
 * //     },
 * //   ],
 * //   Name: "STRING_VALUE",
 * //   Prefix: "STRING_VALUE",
 * //   MaxKeys: Number("int"),
 * // };
 *
 * ```
 *
 * @param ListLifecycleCurrentsCommandInput - {@link ListLifecycleCurrentsCommandInput}
 * @returns {@link ListLifecycleCurrentsCommandOutput}
 * @see {@link ListLifecycleCurrentsCommandInput} for command's `input` shape.
 * @see {@link ListLifecycleCurrentsCommandOutput} for command's `response` shape.
 * @see {@link BackbeatClientResolvedConfig | config} for BackbeatClient's `config` shape.
 *
 * @throws {@link BackbeatServiceException}
 * <p>Base exception class for all service exceptions from Backbeat service.</p>
 *
 *
 * @public
 */
export class ListLifecycleCurrentsCommand extends $Command.classBuilder<ListLifecycleCurrentsCommandInput, ListLifecycleCurrentsCommandOutput, BackbeatClientResolvedConfig, ServiceInputTypes, ServiceOutputTypes>()
  .ep(commonParams)
      .m(function (this: any, Command: any, cs: any, config: BackbeatClientResolvedConfig, o: any) {
          return [

  getSerdePlugin(config, this.serialize, this.deserialize),
  getEndpointPlugin(config, Command.getEndpointParameterInstructions()),
      ];
  })
  .s("Backbeat", "ListLifecycleCurrents", {

  })
  .n("BackbeatClient", "ListLifecycleCurrentsCommand")
  .f(void 0, void 0)
  .ser(se_ListLifecycleCurrentsCommand)
  .de(de_ListLifecycleCurrentsCommand)
.build() {
/** @internal type navigation helper, not in runtime. */
declare protected static __types: {
  api: {
      input: ListLifecycleCurrentsInput;
      output: ListLifecycleCurrentsOutput;
  };
  sdk: {
      input: ListLifecycleCurrentsCommandInput;
      output: ListLifecycleCurrentsCommandOutput;
  };
};
}
