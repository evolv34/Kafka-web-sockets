package com.websockets

trait Connect {
  def connect():Unit;
  def close():Unit;
}