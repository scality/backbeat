// smithy-typescript generated code
import {
  BackbeatClientResolvedConfig,
  ServiceInputTypes,
  ServiceOutputTypes,
} from "../BackbeatClient";
import { commonParams } from "../endpoint/EndpointParameters";
import {
  GetRaftLogInput,
  GetRaftLogOutput,
} from "../models/models_0";
import {
  de_GetRaftLogCommand,
  se_GetRaftLogCommand,
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
 * The input for {@link GetRaftLogCommand}.
 */
export interface GetRaftLogCommandInput extends GetRaftLogInput {}
/**
 * @public
 *
 * The output of {@link GetRaftLogCommand}.
 */
export interface GetRaftLogCommandOutput extends GetRaftLogOutput, __MetadataBearer {}

/**
 * Retrieves Raft log entries for a specific log ID
 * @example
 * Use a bare-bones client and the command you need to make an API call.
 * ```javascript
 * import { BackbeatClient, GetRaftLogCommand } from "@backbeat-service/client"; // ES Modules import
 * // const { BackbeatClient, GetRaftLogCommand } = require("@backbeat-service/client"); // CommonJS import
 * const client = new BackbeatClient(config);
 * const input = { // GetRaftLogInput
 *   LogId: "STRING_VALUE", // required
 *   Begin: Number("int"),
 *   Limit: Number("int"),
 *   TargetLeader: true || false,
 * };
 * const command = new GetRaftLogCommand(input);
 * const response = await client.send(command);
 * // { // GetRaftLogOutput
 * //   info: { // RaftLogInfo
 * //     start: Number("int"),
 * //     cseq: Number("int"),
 * //     prune: Number("int"),
 * //   },
 * //   log: [ // RaftLogEntries
 * //     { // RaftLogEntry
 * //       db: "STRING_VALUE",
 * //       entries: [ // LogEntryList
 * //         { // LogEntryKeyValue
 * //           key: "STRING_VALUE",
 * //           value: "STRING_VALUE",
 * //         },
 * //       ],
 * //     },
 * //   ],
 * // };
 *
 * ```
 *
 * @param GetRaftLogCommandInput - {@link GetRaftLogCommandInput}
 * @returns {@link GetRaftLogCommandOutput}
 * @see {@link GetRaftLogCommandInput} for command's `input` shape.
 * @see {@link GetRaftLogCommandOutput} for command's `response` shape.
 * @see {@link BackbeatClientResolvedConfig | config} for BackbeatClient's `config` shape.
 *
 * @throws {@link BackbeatServiceException}
 * <p>Base exception class for all service exceptions from Backbeat service.</p>
 *
 *
 * @public
 */
export class GetRaftLogCommand extends $Command.classBuilder<GetRaftLogCommandInput, GetRaftLogCommandOutput, BackbeatClientResolvedConfig, ServiceInputTypes, ServiceOutputTypes>()
  .ep(commonParams)
      .m(function (this: any, Command: any, cs: any, config: BackbeatClientResolvedConfig, o: any) {
          return [

  getSerdePlugin(config, this.serialize, this.deserialize),
  getEndpointPlugin(config, Command.getEndpointParameterInstructions()),
      ];
  })
  .s("Backbeat", "GetRaftLog", {

  })
  .n("BackbeatClient", "GetRaftLogCommand")
  .f(void 0, void 0)
  .ser(se_GetRaftLogCommand)
  .de(de_GetRaftLogCommand)
.build() {
/** @internal type navigation helper, not in runtime. */
declare protected static __types: {
  api: {
      input: GetRaftLogInput;
      output: GetRaftLogOutput;
  };
  sdk: {
      input: GetRaftLogCommandInput;
      output: GetRaftLogCommandOutput;
  };
};
}
