#!/usr/bin/env tmanstack
-- stack runghc

{-# LANGUAGE OverloadedStrings #-}
import Tman

main = do
    tman "~/test/tmanstack_logdir" $ do
        addTask $ task "test1" "echo hello world"
        addTask $ task "test2" "echo HELLO WORLD"

