package api

import (
	"encoding/base64"
	"net/http"

	"github.com/WuKongIM/WuKongIM/internal/usecase/message"
	"github.com/WuKongIM/WuKongIM/pkg/wkpacket"
	"github.com/gin-gonic/gin"
)

type sendMessageRequest struct {
	SenderUID   string `json:"sender_uid"`
	ChannelID   string `json:"channel_id"`
	ChannelType uint8  `json:"channel_type"`
	Payload     string `json:"payload"`
}

type sendMessageResponse struct {
	MessageID  int64  `json:"message_id"`
	MessageSeq uint64 `json:"message_seq"`
	Reason     uint8  `json:"reason"`
}

func (s *Server) handleSendMessage(c *gin.Context) {
	var req sendMessageRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		writeJSONError(c, http.StatusBadRequest, "invalid request")
		return
	}
	if req.SenderUID == "" || req.ChannelID == "" || req.ChannelType == 0 || req.Payload == "" {
		writeJSONError(c, http.StatusBadRequest, "invalid request")
		return
	}

	payload, err := base64.StdEncoding.DecodeString(req.Payload)
	if err != nil {
		writeJSONError(c, http.StatusBadRequest, "invalid payload")
		return
	}

	if s == nil || s.messages == nil {
		writeJSONError(c, http.StatusInternalServerError, "message usecase not configured")
		return
	}

	result, err := s.messages.Send(message.SendCommand{
		SenderUID:       req.SenderUID,
		ChannelID:       req.ChannelID,
		ChannelType:     req.ChannelType,
		Payload:         payload,
		ProtocolVersion: wkpacket.LatestVersion,
	})
	if err != nil {
		if status, msg, ok := mapSendError(err); ok {
			writeJSONError(c, status, msg)
			return
		}
		writeJSONError(c, http.StatusInternalServerError, err.Error())
		return
	}

	c.JSON(http.StatusOK, sendMessageResponse{
		MessageID:  result.MessageID,
		MessageSeq: result.MessageSeq,
		Reason:     uint8(result.Reason),
	})
}

func writeJSONError(c *gin.Context, status int, message string) {
	if c == nil {
		return
	}
	if message == "" {
		message = http.StatusText(status)
	}
	c.JSON(status, gin.H{"error": message})
}
