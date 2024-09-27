
#pragma once

#include "cuda_blackboard/cuda_blackboard.hpp"
#include "cuda_blackboard/negotiated_types.hpp"

#include <negotiated/negotiated_publisher.hpp>
#include <rclcpp/rclcpp.hpp>

#include <memory>
#include <string>
#include <utility>

namespace cuda_blackboard
{

template <typename T>
class CudaBlackboardPublisher
{
public:
  CudaBlackboardPublisher(rclcpp::Node & node, const std::string & topic_name) : node_(node)
  {
    negotiated::NegotiatedPublisherOptions negotiation_options;
    negotiation_options.disconnect_publishers_on_failure = false;

    negotiated_pub_ = std::make_shared<negotiated::NegotiatedPublisher>(
      node, topic_name + "/cuda", negotiation_options);
    rclcpp::PublisherOptions pub_options;
    pub_options.use_intra_process_comm = rclcpp::IntraProcessSetting::Enable;

    negotiated_pub_->add_supported_type<NegotiationStruct<T>>(
      1.0,
      rclcpp::QoS(1),  //.durability_volatile(),
      pub_options);

    std::string ros_type_name = NegotiationStruct<typename T::ros_type>::supported_type_name;
    compatible_pub_ =
      node.create_publisher<typename T::ros_type>(topic_name, rclcpp::SensorDataQoS(), pub_options);
    negotiated_pub_->add_compatible_publisher(compatible_pub_, ros_type_name, 0.1);

    negotiated_pub_->start();
  }

  void publish(std::unique_ptr<const T> cuda_msg_ptr)
  {
    auto & map = negotiated_pub_->get_supported_types();

    using ROSMessageType = typename NegotiationStruct<T>::MsgT;
    std::string ros_type_name = rosidl_generator_traits::name<ROSMessageType>();
    std::string key_name =
      negotiated::detail::generate_key(ros_type_name, NegotiationStruct<T>::supported_type_name);

    std::unique_ptr<typename T::ros_type> ros_msg_ptr;

    // Note: it is better to publish the blackboard first, since ros data can take longer...
    if (
      negotiated_pub_->type_was_negotiated<NegotiationStruct<typename T::ros_type>>() &&
      (compatible_pub_->get_subscription_count() > 0 ||
       compatible_pub_->get_intra_process_subscription_count() > 0)) {
      // Using the standard adaptation pipeline was creating a copy of the data, which is not what
      // we want
      ros_msg_ptr = std::make_unique<typename T::ros_type>();
      rclcpp::TypeAdapter<T>::convert_to_ros_message(*cuda_msg_ptr, *ros_msg_ptr);
    }

    // When we want to publish cuda data, we instead use the blackboard
    if (negotiated_pub_->type_was_negotiated<NegotiationStruct<T>>() && map.count(key_name) > 0) {
      auto & publisher = map.at(key_name).publisher;
      std::size_t tickets =
        publisher->get_intra_process_subscription_count();  // tickets are only given to intra
                                                            // process subscribers

      if (tickets == 0) {
        return;
      }

      auto & blackboard = CudaBlackboard<T>::getInstance();
      uint64_t instance_id = blackboard.registerData(
        std::string(node_.get_fully_qualified_name()) + "_" + publisher->get_topic_name(),
        std::move(cuda_msg_ptr), tickets);

      RCLCPP_DEBUG(
        node_.get_logger(), "Publishing instance id %lu with %ld tickets", instance_id, tickets);

      auto instance_msg = std_msgs::msg::UInt64();
      instance_msg.data = static_cast<uint64_t>(instance_id);
      negotiated_pub_->publish<NegotiationStruct<T>>(instance_msg);
    }

    // When we want to publish ros data, we need to use type adaptation
    if (
      negotiated_pub_->type_was_negotiated<NegotiationStruct<typename T::ros_type>>() &&
      (compatible_pub_->get_subscription_count() > 0 ||
       compatible_pub_->get_intra_process_subscription_count() > 0)) {
      compatible_pub_->publish(std::move(ros_msg_ptr));
    }
  }

  void publish(std::unique_ptr<typename T::ros_type> ros_msg_ptr)
  {
    if (negotiated_pub_->type_was_negotiated<NegotiationStruct<T>>()) {
      auto cuda_msg_ptr = std::make_unique<T>();
      rclcpp::TypeAdapter<T>::convert_to_custom(*ros_msg_ptr, *cuda_msg_ptr);
      std::unique_ptr<const T> const_cuda_msg_ptr = std::move(cuda_msg_ptr);
      return publish(std::move(const_cuda_msg_ptr));
    }

    if (
      negotiated_pub_->type_was_negotiated<NegotiationStruct<typename T::ros_type>>() &&
      (compatible_pub_->get_subscription_count() > 0 ||
       compatible_pub_->get_intra_process_subscription_count() > 0)) {
      compatible_pub_->publish(std::move(ros_msg_ptr));
    }
  }

  std::size_t get_subscription_count() const
  {
    auto & map = negotiated_pub_->get_supported_types();
    return std::accumulate(map.begin(), map.end(), 0, [](int count, const auto & p) {
      std::size_t sub_count = p.second.publisher ? p.second.publisher->get_subscription_count() : 0;
      return count + sub_count;
    });
  }
  std::size_t get_intra_process_subscription_count() const
  {
    auto & map = negotiated_pub_->get_supported_types();
    return std::accumulate(map.begin(), map.end(), 0, [](int count, const auto & p) {
      std::size_t sub_count =
        p.second.publisher ? p.second.publisher->get_intra_process_subscription_count() : 0;
      return count + sub_count;
    });
  }

private:
  rclcpp::Node & node_;
  std::shared_ptr<negotiated::NegotiatedPublisher> negotiated_pub_;
  typename rclcpp::Publisher<typename T::ros_type>::SharedPtr compatible_pub_;
};

}  // namespace cuda_blackboard
