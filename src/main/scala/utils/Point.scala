package utils

/**
 * Class that wraps data extracted by .seq file.
 * @param id Int id of the point
 * @param vector Array of Bytes with the coordinates of the Point
 */
case class Point(id: Int, vector: Array[Byte]) extends Serializable
