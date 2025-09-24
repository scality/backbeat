// smithy-typescript generated code
import {
  BackbeatClientResolvedConfig,
  ServiceInputTypes,
  ServiceOutputTypes,
} from "../BackbeatClient";
import { commonParams } from "../endpoint/EndpointParameters";
import {
  ListLifecycleNonCurrentsInput,
  ListLifecycleNonCurrentsOutput,
} from "../models/models_0";
import {
  de_ListLifecycleNonCurrentsCommand,
  se_ListLifecycleNonCurrentsCommand,
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
 * The input for {@link ListLifecycleNonCurrentsCommand}.
 */
export interface ListLifecycleNonCurrentsCommandInput extends ListLifecycleNonCurrentsInput {}
/**
 * @public
 *
 * The output of {@link ListLifecycleNonCurrentsCommand}.
 */
export interface ListLifecycleNonCurrentsCommandOutput extends ListLifecycleNonCurrentsOutput, __MetadataBearer {}

/**
 * List lifecycle non-current objects operation
 * @example
 * Use a bare-bones client and the command you need to make an API call.
 * ```javascript
 * import { BackbeatClient, ListLifecycleNonCurrentsCommand } from "@backbeat-service/client"; // ES Modules import
 * // const { BackbeatClient, ListLifecycleNonCurrentsCommand } = require("@backbeat-service/client"); // CommonJS import
 * const client = new BackbeatClient(config);
 * const input = { // ListLifecycleNonCurrentsInput
 *   Bucket: "STRING_VALUE", // required
 *   BeforeDate: "STRING_VALUE",
 *   ExcludedDataStoreName: "STRING_VALUE",
 *   EncodingType: "STRING_VALUE",
 *   KeyMarker: "STRING_VALUE",
 *   VersionIdMarker: "STRING_VALUE",
 *   MaxKeys: Number("int"),
 *   Prefix: "STRING_VALUE",
 * };
 * const command = new ListLifecycleNonCurrentsCommand(input);
 * const response = await client.send(command);
 * // { // ListLifecycleNonCurrentsOutput
 * //   BeforeDate: "STRING_VALUE",
 * //   KeyMarker: "STRING_VALUE",
 * //   VersionIdMarker: "STRING_VALUE",
 * //   IsTruncated: true || false,
 * //   NextKeyMarker: "STRING_VALUE",
 * //   NextVersionIdMarker: "STRING_VALUE",
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
 * @param ListLifecycleNonCurrentsCommandInput - {@link ListLifecycleNonCurrentsCommandInput}
 * @returns {@link ListLifecycleNonCurrentsCommandOutput}
 * @see {@link ListLifecycleNonCurrentsCommandInput} for command's `input` shape.
 * @see {@link ListLifecycleNonCurrentsCommandOutput} for command's `response` shape.
 * @see {@link BackbeatClientResolvedConfig | config} for BackbeatClient's `config` shape.
 *
 * @throws {@link BackbeatServiceException}
 * <p>Base exception class for all service exceptions from Backbeat service.</p>
 *
 *
 * @public
 */
export class ListLifecycleNonCurrentsCommand extends $Command.classBuilder<ListLifecycleNonCurrentsCommandInput, ListLifecycleNonCurrentsCommandOutput, BackbeatClientResolvedConfig, ServiceInputTypes, ServiceOutputTypes>()
  .ep(commonParams)
      .m(function (this: any, Command: any, cs: any, config: BackbeatClientResolvedConfig, o: any) {
          return [

  getSerdePlugin(config, this.serialize, this.deserialize),
  getEndpointPlugin(config, Command.getEndpointParameterInstructions()),
      ];
  })
  .s("Backbeat", "ListLifecycleNonCurrents", {

  })
  .n("BackbeatClient", "ListLifecycleNonCurrentsCommand")
  .f(void 0, void 0)
  .ser(se_ListLifecycleNonCurrentsCommand)
  .de(de_ListLifecycleNonCurrentsCommand)
.build() {
/** @internal type navigation helper, not in runtime. */
declare protected static __types: {
  api: {
      input: ListLifecycleNonCurrentsInput;
      output: ListLifecycleNonCurrentsOutput;
  };
  sdk: {
      input: ListLifecycleNonCurrentsCommandInput;
      output: ListLifecycleNonCurrentsCommandOutput;
  };
};
}
