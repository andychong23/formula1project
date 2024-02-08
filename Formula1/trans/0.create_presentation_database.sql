-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS f1_presentation
-- This sets up such that the data written will be stored to our abfs instead of the warehouse in databricks
LOCATION 'abfss://presentation@newdatabrickscoursedl.dfs.core.windows.net/'
