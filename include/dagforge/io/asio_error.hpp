#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include "dagforge/io/result.hpp"

#include <boost/asio/error.hpp>
#include <boost/system/error_code.hpp>
#endif

namespace dagforge::io {

[[nodiscard]] inline auto
normalize_error_code(const boost::system::error_code &error) noexcept
    -> std::error_code {
  if (!error) {
    return {};
  }
  if (error == boost::asio::error::operation_aborted) {
    return make_error_code(IoError::Cancelled);
  }
  if (error == boost::asio::error::timed_out) {
    return make_error_code(IoError::TimedOut);
  }
  if (error == boost::asio::error::eof) {
    return make_error_code(IoError::EndOfFile);
  }
  if (error == boost::asio::error::connection_reset) {
    return make_error_code(IoError::ConnectionReset);
  }
  if (error == boost::asio::error::connection_refused) {
    return make_error_code(IoError::ConnectionRefused);
  }
  if (error == boost::asio::error::broken_pipe) {
    return make_error_code(IoError::BrokenPipe);
  }
  if (error == boost::asio::error::would_block ||
      error == boost::asio::error::try_again) {
    return make_error_code(IoError::WouldBlock);
  }
  if (error == boost::asio::error::invalid_argument) {
    return make_error_code(IoError::InvalidArgument);
  }
  if (error == boost::asio::error::bad_descriptor) {
    return make_error_code(IoError::BadDescriptor);
  }
  if (error == boost::asio::error::no_buffer_space) {
    return make_error_code(IoError::NoBufferSpace);
  }
  if (error == boost::asio::error::in_progress ||
      error == boost::asio::error::already_started) {
    return make_error_code(IoError::OperationInProgress);
  }
  if (error == boost::asio::error::not_connected) {
    return make_error_code(IoError::NotConnected);
  }
  if (error == boost::asio::error::already_connected) {
    return make_error_code(IoError::AlreadyConnected);
  }
  return error;
}

} // namespace dagforge::io
