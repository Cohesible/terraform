// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package json

type MessageType string

const (
	// Session messages
	MessageReady MessageType = "ready"

	// Generic messages
	MessageVersion    MessageType = "version"
	MessageLog        MessageType = "log"
	MessageDiagnostic MessageType = "diagnostic"

	// Operation results
	MessageResourceDrift MessageType = "resource_drift"
	MessagePlannedChange MessageType = "planned_change"
	MessageChangeSummary MessageType = "change_summary"
	MessageOutputs       MessageType = "outputs"
	MessageResult        MessageType = "result"

	// Hook-driven messages
	MessageApplyStart        MessageType = "apply_start"
	MessageApplyProgress     MessageType = "apply_progress"
	MessageApplyComplete     MessageType = "apply_complete"
	MessageApplyErrored      MessageType = "apply_errored"
	MessageProvisionStart    MessageType = "provision_start"
	MessageProvisionProgress MessageType = "provision_progress"
	MessageProvisionComplete MessageType = "provision_complete"
	MessageProvisionErrored  MessageType = "provision_errored"
	MessageRefreshStart      MessageType = "refresh_start"
	MessageRefreshComplete   MessageType = "refresh_complete"
)
