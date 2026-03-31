cmake_minimum_required(VERSION 3.0.2)
project(ros_ntrip_client)

add_compile_options(-std=c++14 -Wall -Wextra -Wpedantic)

find_package(catkin REQUIRED COMPONENTS
  nmea_msgs
  roscpp
  rtcm_msgs
  std_msgs
)
find_package(OpenSSL REQUIRED)

catkin_package(
  INCLUDE_DIRS include
  LIBRARIES ntrip_client
  CATKIN_DEPENDS nmea_msgs roscpp rtcm_msgs std_msgs
)

include_directories(
  include
  ${catkin_INCLUDE_DIRS}
)

add_library(ntrip_client
  src/ntrip_client.cpp
  src/ntrip_transport.cpp
  src/rtcm_framer.cpp
  src/status_code.cpp
)
target_link_libraries(ntrip_client
  OpenSSL::SSL
  OpenSSL::Crypto
  ${catkin_LIBRARIES}
)

add_executable(ntrip_client_node
  src/ntrip_client_node.cpp
  src/ntrip_client_node_utils.cpp
)
target_link_libraries(ntrip_client_node
  ntrip_client
  ${catkin_LIBRARIES}
)

install(TARGETS ntrip_client ntrip_client_node
  ARCHIVE DESTINATION ${CATKIN_PACKAGE_LIB_DESTINATION}
  LIBRARY DESTINATION ${CATKIN_PACKAGE_LIB_DESTINATION}
  RUNTIME DESTINATION ${CATKIN_PACKAGE_BIN_DESTINATION}
)

install(DIRECTORY include/${PROJECT_NAME}/
  DESTINATION ${CATKIN_PACKAGE_INCLUDE_DESTINATION}
)

install(DIRECTORY launch config
  DESTINATION ${CATKIN_PACKAGE_SHARE_DESTINATION}
)

if(CATKIN_ENABLE_TESTING)
  find_package(GTest REQUIRED)
  include_directories(${GTEST_INCLUDE_DIRS})

  catkin_add_gtest(ntrip_client_test
    test/test_ntrip_client.cpp
  )
  if(TARGET ntrip_client_test)
    target_link_libraries(ntrip_client_test
      ntrip_client
      OpenSSL::SSL
      OpenSSL::Crypto
      ${catkin_LIBRARIES}
      ${GTEST_LIBRARIES}
      pthread
    )
    if(TARGET GTest::Main)
      target_link_libraries(ntrip_client_test GTest::Main)
    elseif(GTEST_MAIN_LIBRARIES)
      target_link_libraries(ntrip_client_test ${GTEST_MAIN_LIBRARIES})
    endif()
  endif()
endif()
