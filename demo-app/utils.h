#ifndef UTILS_H
#define UTILS_H

#include <utility>
#include <string>
#include <sstream>
#include <iostream>

#include <boost/date_time.hpp>


namespace str
{

namespace details
{

template <typename T>
std::ostream &buildStr(std::ostream &oss, T &&x)
{
    return oss << std::forward<T>(x);
}

template <typename T, typename... Args>
std::ostream &buildStr(std::ostream &oss, T &&x, Args &&... args)
{
    return buildStr(oss << std::forward<T>(x), std::forward<Args>(args)...);
}

template <typename Sep, typename T>
std::ostream &buildStrWithSep(std::ostream &oss, Sep &&, T &&x)
{
    return oss << std::forward<T>(x);
}

template <typename Sep, typename T, typename... Args>
std::ostream &buildStrWithSep(std::ostream &oss, Sep &&sep, T &&x, Args &&... args)
{
    return buildStrWithSep(oss << std::forward<T>(x) << std::forward<Sep>(sep),
                                  std::forward<Sep>(sep),
                                  std::forward<Args>(args)...);
}

} // namespace details

/**
 * Создание строки из набора разнотипных аргументов.
 * @return строка, собранная из аргуметов
 */
template <typename... Args>
std::string concat(Args &&... args)
{
    std::ostringstream oss;
    details::buildStr(oss, std::forward<Args>(args)...);
    return oss.str();
}

/**
 * Создание строки из набора разнотипных аргументов, используя разделитель
 * @return строка, собранная из аргуметов
 */
template <typename T, typename... Args>
std::string concat_via(T &&separator, Args &&... args)
{
    std::ostringstream oss;
    details::buildStrWithSep(oss, separator, std::forward<Args>(args)...);
    return oss.str();
}

/**
 * Создание строки из набора разнотипных аргументов, используя пробел в кач-ве разделителя
 * @return строка, собранная из аргуметов
 */
template <typename... Args>
std::string concat_ws(Args &&... args)
{
    return concat_via(' ', std::forward<Args>(args)...);
}

} // namespace str

namespace utils
{

template <typename... Args>
void log_to_stream(std::ostream &os, Args &&... args)
{
    os << '[' << boost::posix_time::second_clock::local_time() << "] ";
    str::details::buildStrWithSep(os, ' ', std::forward<Args>(args)...);
    os << '\n';
}

template <typename... Args>
void log_info(Args &&... args)
{
    std::cerr << "\033[1;32m";
    log_to_stream(std::cerr, std::forward<Args>(args)...);
    std::cerr << "\033[0m";
}

template <typename... Args>
void log_err(Args &&... args)
{
    std::cerr << "\033[1;31m";
    log_to_stream(std::cerr, std::forward<Args>(args)...);
    std::cerr << "\033[0m";
}

} //namespace utils

#endif // COMMON_H

