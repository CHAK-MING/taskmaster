QueryParams::QueryParams(std::string_view query_string) {
  if (query_string.empty()) {
    return;
  }

  const auto parsed = boost::urls::parse_query(query_string);
  if (!parsed) {
    return;
  }

  for (const auto &param : *parsed) {
    const auto decoded_key = param.key.decode();
    std::string key(decoded_key.begin(), decoded_key.end());
    std::string value;
    if (param.has_value) {
      const auto decoded_value = param.value.decode();
      value.assign(decoded_value.begin(), decoded_value.end());
    }
    params_[std::move(key)] = std::move(value);
  }
}
