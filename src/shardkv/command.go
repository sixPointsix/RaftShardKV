package shardkv

import (
	"fmt"

	"github.com/sixPointsix/RaftShardKV/shardctrler"
)

type Command struct {
	Op   CommandType
	Data interface{}
}

func (command Command) String() string {
	return fmt.Sprintf("{Type:%v,Data:%v}", command.Op, command.Data)
}

func NewOperationCommand(args *CmdArgs) Command {
	return Command{Operation, *args}
}

func NewConfigurationCommand(config *shardctrler.Config) Command {
	return Command{Configuration, *config}
}

func NewInsertShardsCommand(pullReply *PullDataReply) Command {
	return Command{InsertShards, *pullReply}
}

func NewDeleteShardsCommand(pullArgs *PullDataArgs) Command {
	return Command{DeleteShards, *pullArgs}
}

type CommandType uint8

const (
	Operation CommandType = iota
	Configuration
	InsertShards
	DeleteShards
)
