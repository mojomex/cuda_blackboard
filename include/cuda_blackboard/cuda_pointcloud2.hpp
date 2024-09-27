
#pragma once

#include <rclcpp/rclcpp.hpp>
#include <sensor_msgs/msg/point_cloud2.hpp>

#include <cuda_runtime_api.h>

#include <memory>

namespace cuda_blackboard
{

class CudaPointCloud2 : public sensor_msgs::msg::PointCloud2
{
public:
  using ros_type = sensor_msgs::msg::PointCloud2;

  CudaPointCloud2() = default;
  CudaPointCloud2(const CudaPointCloud2 & pointcloud) : sensor_msgs::msg::PointCloud2(pointcloud)
  {
    RCLCPP_WARN(
      rclcpp::get_logger("CudaPointCloud2"),
      "CudaPointCloud2 copy constructor called. This should be avoided and is most likely a design "
      "error.");

    cudaMalloc(
      reinterpret_cast<void **>(&data),
      pointcloud.height * pointcloud.width * pointcloud.point_step * sizeof(uint8_t));
    cudaMemcpy(
      data, pointcloud.data,
      pointcloud.height * pointcloud.width * pointcloud.point_step * sizeof(uint8_t),
      cudaMemcpyDeviceToDevice);
  }

  CudaPointCloud2 & operator=(const CudaPointCloud2 &) = delete;

  CudaPointCloud2(CudaPointCloud2 && source)
  {
    header = source.header;
    fields = source.fields;
    height = source.height;
    width = source.width;
    row_step = source.row_step;
    point_step = source.point_step;
    is_dense = source.is_dense;
    is_bigendian = source.is_bigendian;
    data = source.data;

    source.data = nullptr;
  }

  CudaPointCloud2 & operator=(CudaPointCloud2 && other)
  {
    if (this != &other) {
      header = other.header;
      fields = other.fields;
      height = other.height;
      width = other.width;
      row_step = other.row_step;
      point_step = other.point_step;
      is_dense = other.is_dense;
      is_bigendian = other.is_bigendian;
      data = other.data;

      other.data = nullptr;
    }

    return *this;
  }

  CudaPointCloud2(const sensor_msgs::msg::PointCloud2 & source)
  {
    header = source.header;
    fields = source.fields;
    height = source.height;
    width = source.width;
    row_step = source.row_step;
    point_step = source.point_step;
    is_dense = source.is_dense;
    is_bigendian = source.is_bigendian;

    cudaMalloc(
      reinterpret_cast<void **>(&data),
      source.height * source.width * source.point_step * sizeof(uint8_t));
    cudaMemcpy(
      data, source.data.data(), source.height * source.width * source.point_step * sizeof(uint8_t),
      cudaMemcpyHostToDevice);
  }

  ~CudaPointCloud2() { cudaFree(data); }

  uint8_t * data{nullptr};
};

}  // namespace cuda_blackboard
