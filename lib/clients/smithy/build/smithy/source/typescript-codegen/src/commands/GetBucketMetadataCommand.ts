// smithy-typescript generated code
import {
  BackbeatClientResolvedConfig,
  ServiceInputTypes,
  ServiceOutputTypes,
} from "../BackbeatClient";
import { commonParams } from "../endpoint/EndpointParameters";
import {
  GetBucketMetadataInput,
  GetBucketMetadataOutput,
} from "../models/models_0";
import {
  de_GetBucketMetadataCommand,
  se_GetBucketMetadataCommand,
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
 * The input for {@link GetBucketMetadataCommand}.
 */
export interface GetBucketMetadataCommandInput extends GetBucketMetadataInput {}
/**
 * @public
 *
 * The output of {@link GetBucketMetadataCommand}.
 */
export interface GetBucketMetadataCommandOutput extends GetBucketMetadataOutput, __MetadataBearer {}

/**
 * @public
 *
 * @example
 * Use a bare-bones client and the command you need to make an API call.
 * ```javascript
 * import { BackbeatClient, GetBucketMetadataCommand } from "@backbeat-service/client"; // ES Modules import
 * // const { BackbeatClient, GetBucketMetadataCommand } = require("@backbeat-service/client"); // CommonJS import
 * const client = new BackbeatClient(config);
 * const input = { // GetBucketMetadataInput
 *   Bucket: "STRING_VALUE", // required
 * };
 * const command = new GetBucketMetadataCommand(input);
 * const response = await client.send(command);
 * // { // GetBucketMetadataOutput
 * //   acl: { // AclObj
 * //     Canned: "STRING_VALUE",
 * //     FULL_CONTROL: [ // StringList
 * //       "STRING_VALUE",
 * //     ],
 * //     WRITE: [
 * //       "STRING_VALUE",
 * //     ],
 * //     WRITE_ACP: [
 * //       "STRING_VALUE",
 * //     ],
 * //     READ: [
 * //       "STRING_VALUE",
 * //     ],
 * //     READ_ACP: [
 * //       "STRING_VALUE",
 * //     ],
 * //   },
 * //   name: "STRING_VALUE",
 * //   owner: "STRING_VALUE",
 * //   ownerDisplayName: "STRING_VALUE",
 * //   creationDate: "STRING_VALUE",
 * //   mdBucketModelVersion: Number("int"),
 * //   transient: true || false,
 * //   deleted: true || false,
 * //   serverSideEncryption: { // ServerSideEncryptionMap
 * //     "<keys>": "STRING_VALUE",
 * //   },
 * //   versioningConfiguration: { // VersioningConfigurationObj
 * //     "<keys>": "STRING_VALUE",
 * //   },
 * //   locationConstraint: "STRING_VALUE",
 * //   readLocationConstraint: "STRING_VALUE",
 * //   cors: [ // CorsListObj
 * //     { // CorsObj
 * //       "<keys>": "STRING_VALUE",
 * //     },
 * //   ],
 * //   replicationConfiguration: { // ReplicationConfigurationObj
 * //     "<keys>": "STRING_VALUE",
 * //   },
 * //   lifecycleConfiguration: { // LifecycleConfigurationObj
 * //     Rules: [ // LifecycleRuleList
 * //       { // LCRuleObj
 * //         ID: "STRING_VALUE",
 * //         Status: "Enabled" || "Disabled",
 * //         Prefix: "STRING_VALUE",
 * //         Expiration: { // ExpirationConfiguration
 * //           Days: Number("int"),
 * //         },
 * //       },
 * //     ],
 * //   },
 * //   uid: "STRING_VALUE",
 * // };
 *
 * ```
 *
 * @param GetBucketMetadataCommandInput - {@link GetBucketMetadataCommandInput}
 * @returns {@link GetBucketMetadataCommandOutput}
 * @see {@link GetBucketMetadataCommandInput} for command's `input` shape.
 * @see {@link GetBucketMetadataCommandOutput} for command's `response` shape.
 * @see {@link BackbeatClientResolvedConfig | config} for BackbeatClient's `config` shape.
 *
 * @throws {@link BackbeatServiceException}
 * <p>Base exception class for all service exceptions from Backbeat service.</p>
 *
 *
 */
export class GetBucketMetadataCommand extends $Command.classBuilder<GetBucketMetadataCommandInput, GetBucketMetadataCommandOutput, BackbeatClientResolvedConfig, ServiceInputTypes, ServiceOutputTypes>()
  .ep(commonParams)
      .m(function (this: any, Command: any, cs: any, config: BackbeatClientResolvedConfig, o: any) {
          return [

  getSerdePlugin(config, this.serialize, this.deserialize),
  getEndpointPlugin(config, Command.getEndpointParameterInstructions()),
      ];
  })
  .s("Backbeat", "GetBucketMetadata", {

  })
  .n("BackbeatClient", "GetBucketMetadataCommand")
  .f(void 0, void 0)
  .ser(se_GetBucketMetadataCommand)
  .de(de_GetBucketMetadataCommand)
.build() {
/** @internal type navigation helper, not in runtime. */
declare protected static __types: {
  api: {
      input: GetBucketMetadataInput;
      output: GetBucketMetadataOutput;
  };
  sdk: {
      input: GetBucketMetadataCommandInput;
      output: GetBucketMetadataCommandOutput;
  };
};
}
