// smithy-typescript generated code
import {
  BackbeatClientResolvedConfig,
  ServiceInputTypes,
  ServiceOutputTypes,
} from "../BackbeatClient";
import { commonParams } from "../endpoint/EndpointParameters";
import {
  GetObjectInput,
  GetObjectOutput,
} from "../models/models_0";
import {
  de_GetObjectCommand,
  se_GetObjectCommand,
} from "../protocols/Aws_restJson1";
import { getEndpointPlugin } from "@smithy/middleware-endpoint";
import { getSerdePlugin } from "@smithy/middleware-serde";
import { Command as $Command } from "@smithy/smithy-client";
import { MetadataBearer as __MetadataBearer } from "@smithy/types";
import { Uint8ArrayBlobAdapter } from "@smithy/util-stream";

/**
 * @public
 */
export type { __MetadataBearer };
export { $Command };
/**
 * @public
 *
 * The input for {@link GetObjectCommand}.
 */
export interface GetObjectCommandInput extends GetObjectInput {}
/**
 * @public
 */
export type GetObjectCommandOutputType = Omit<GetObjectOutput, "Body"> & {
  Body?: Uint8ArrayBlobAdapter;
};

/**
 * @public
 *
 * The output of {@link GetObjectCommand}.
 */
export interface GetObjectCommandOutput extends GetObjectCommandOutputType, __MetadataBearer {}

/**
 * @public
 *
 * @example
 * Use a bare-bones client and the command you need to make an API call.
 * ```javascript
 * import { BackbeatClient, GetObjectCommand } from "@backbeat-service/client"; // ES Modules import
 * // const { BackbeatClient, GetObjectCommand } = require("@backbeat-service/client"); // CommonJS import
 * const client = new BackbeatClient(config);
 * const input = { // GetObjectInput
 *   Bucket: "STRING_VALUE", // required
 *   Key: "STRING_VALUE", // required
 *   VersionId: "STRING_VALUE",
 *   CanonicalID: "STRING_VALUE",
 * };
 * const command = new GetObjectCommand(input);
 * const response = await client.send(command);
 * // { // GetObjectOutput
 * //   ContentType: "STRING_VALUE",
 * //   ETag: "STRING_VALUE",
 * //   LastModified: new Date("TIMESTAMP"),
 * //   VersionId: "STRING_VALUE",
 * //   Body: new Uint8Array(),
 * // };
 *
 * ```
 *
 * @param GetObjectCommandInput - {@link GetObjectCommandInput}
 * @returns {@link GetObjectCommandOutput}
 * @see {@link GetObjectCommandInput} for command's `input` shape.
 * @see {@link GetObjectCommandOutput} for command's `response` shape.
 * @see {@link BackbeatClientResolvedConfig | config} for BackbeatClient's `config` shape.
 *
 * @throws {@link BackbeatServiceException}
 * <p>Base exception class for all service exceptions from Backbeat service.</p>
 *
 *
 */
export class GetObjectCommand extends $Command.classBuilder<GetObjectCommandInput, GetObjectCommandOutput, BackbeatClientResolvedConfig, ServiceInputTypes, ServiceOutputTypes>()
  .ep(commonParams)
      .m(function (this: any, Command: any, cs: any, config: BackbeatClientResolvedConfig, o: any) {
          return [

  getSerdePlugin(config, this.serialize, this.deserialize),
  getEndpointPlugin(config, Command.getEndpointParameterInstructions()),
      ];
  })
  .s("Backbeat", "GetObject", {

  })
  .n("BackbeatClient", "GetObjectCommand")
  .f(void 0, void 0)
  .ser(se_GetObjectCommand)
  .de(de_GetObjectCommand)
.build() {
/** @internal type navigation helper, not in runtime. */
declare protected static __types: {
  api: {
      input: GetObjectInput;
      output: GetObjectOutput;
  };
  sdk: {
      input: GetObjectCommandInput;
      output: GetObjectCommandOutput;
  };
};
}
