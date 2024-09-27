
#pragma once

#include "cuda_blackboard/cuda_image.hpp"
#include "cuda_blackboard/cuda_pointcloud2.hpp"

#include <memory>
#include <mutex>
#include <random>
#include <string>
#include <unordered_map>

namespace cuda_blackboard
{

template <typename T>
class CudaBlackboardDataWrapper
{
public:
  CudaBlackboardDataWrapper(
    std::shared_ptr<const T> data_ptr, const std::string producer_name, uint64_t instance_id,
    std::size_t tickets)
  : data_ptr_(data_ptr), producer_name_(producer_name), instance_id_(instance_id), tickets_(tickets)
  {
  }
  std::shared_ptr<const T> data_ptr_;
  std::string producer_name_;
  uint64_t instance_id_;
  std::size_t tickets_;
};

template <typename T>
class CudaBlackboard
{
public:
  using DataUniquePtrConst = std::unique_ptr<const T>;
  using DataPtrConst = std::shared_ptr<const T>;
  using CudaBlackboardDataWrapperPtr = std::shared_ptr<CudaBlackboardDataWrapper<T>>;

  static CudaBlackboard & getInstance()
  {
    static CudaBlackboard instance;
    return instance;
  }

  uint64_t registerData(
    const std::string & producer_name, std::unique_ptr<const T> data, std::size_t tickets)
  {
    std::lock_guard<std::mutex> lock(mutex_);

    std::mt19937_64 gen(rd_());
    std::uniform_int_distribution<uint64_t> dist(0, UINT64_MAX);
    uint64_t instance_id = dist(gen);

    while (instance_id_to_data_map_.count(instance_id) > 0) {
      instance_id = dist(gen);
    }

    std::shared_ptr<CudaBlackboardDataWrapper<T>> data_wrapper =
      std::make_shared<CudaBlackboardDataWrapper<T>>(
        std::move(data), producer_name, instance_id, tickets);

    if (producer_to_data_map_.count(producer_name) > 0) {
      RCLCPP_WARN_STREAM(
        rclcpp::get_logger("CudaBlackboard"),
        "Producer " << producer_name << " already exists. Deleting. It had "
                    << producer_to_data_map_[producer_name]->tickets_ << " tickets left");
      instance_id_to_data_map_.erase(producer_to_data_map_[producer_name]->instance_id_);
      producer_to_data_map_.erase(producer_name);
    }

    RCLCPP_DEBUG(
      rclcpp::get_logger("CudaBlackboard"),
      "Registering data from producer %s with instance id %lu", producer_name.c_str(), instance_id);
    producer_to_data_map_[producer_name] = data_wrapper;
    instance_id_to_data_map_[instance_id] = data_wrapper;

    return instance_id;
  }

  std::shared_ptr<const T> queryData(const std::string & producer_name)
  {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = producer_to_data_map_.find(producer_name);

    if (it != producer_to_data_map_.end()) {
      it->second->tickets_--;
      auto data = it->second->data_ptr_;

      RCLCPP_DEBUG_STREAM(
        rclcpp::get_logger("CudaBlackboard"),
        "Producer " << producer_name << " has " << it->second->tickets_ << " tickets left");

      if (it->second->tickets_ == 0) {
        RCLCPP_DEBUG_STREAM(
          rclcpp::get_logger("CudaBlackboard"), "Removing data from producer " << producer_name);
        instance_id_to_data_map_.erase(it->second->instance_id_);
        producer_to_data_map_.erase(it);
      }

      return data;
    }
    return std::shared_ptr<const T>{};  // Indicate that the key was not found
  }

  std::shared_ptr<const T> queryData(uint64_t instance_id)
  {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = instance_id_to_data_map_.find(instance_id);

    if (it != instance_id_to_data_map_.end()) {
      it->second->tickets_--;
      std::shared_ptr<const T> data = it->second->data_ptr_;

      RCLCPP_DEBUG_STREAM(
        rclcpp::get_logger("CudaBlackboard"),
        "Instance " << instance_id << " has " << it->second->tickets_ << " tickets left");

      if (it->second->tickets_ == 0) {
        RCLCPP_DEBUG_STREAM(
          rclcpp::get_logger("CudaBlackboard"), "Removing data from instance " << instance_id);
        producer_to_data_map_.erase(it->second->producer_name_);
        instance_id_to_data_map_.erase(it);
      }

      assert(data);
      return data;
    }

    return std::shared_ptr<const T>{};  // Indicate that the key was not found
  }

private:
  std::unordered_map<std::string, CudaBlackboardDataWrapperPtr> producer_to_data_map_;
  std::unordered_map<uint64_t, CudaBlackboardDataWrapperPtr> instance_id_to_data_map_;
  std::mutex mutex_;

  CudaBlackboard() {}
  CudaBlackboard(const CudaBlackboard &) = delete;
  CudaBlackboard & operator=(const CudaBlackboard &) = delete;

  std::random_device rd_;
};

}  // namespace cuda_blackboard
