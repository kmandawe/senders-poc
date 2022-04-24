package com.cheetahdigital.senderspoc.service.redisqueues.lua;

public enum LuaScript {
    CHECK("redis_queues_check.lua"),
    MLLEN( "redis_queues_mllen.lua");

    private String file;

    LuaScript(String file) {
        this.file = file;
    }

    public String getFile() {
        return file;
    }
}
