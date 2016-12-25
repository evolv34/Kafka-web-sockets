package com.websockets.connectors

trait Connect {
  def connect():Unit;
  def close():Unit;
}