package helps

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"image"
	"image/color"
	"image/color/palette"
	"image/draw"
	"image/gif"
	"io"

	xdraw "golang.org/x/image/draw"
)

const ChatGPTWebMaxGIFFrames = 64

type ChatGPTWebGIFControls struct {
	FrameCount      int
	FrameDurationMS int
}

// ChatGPTWebGIFInfo bounds allocation before the GIF decoder allocates frames.
type ChatGPTWebGIFInfo struct {
	Width, Height, Frames int
	Pixels                int64
}

func InspectChatGPTWebGIF(ctx context.Context, input io.Reader, maxPixels int64) (ChatGPTWebGIFInfo, error) {
	var info ChatGPTWebGIFInfo
	r := bufio.NewReader(input)
	var header [13]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return info, err
	}
	if string(header[:6]) != "GIF87a" && string(header[:6]) != "GIF89a" {
		return info, errors.New("invalid GIF signature")
	}
	info.Width, info.Height = int(binary.LittleEndian.Uint16(header[6:8])), int(binary.LittleEndian.Uint16(header[8:10]))
	if info.Width == 0 || info.Height == 0 || int64(info.Width)*int64(info.Height) > maxPixels {
		return info, errors.New("GIF canvas exceeds pixel budget")
	}
	skipPalette := func(flags byte) error {
		if flags&0x80 == 0 {
			return nil
		}
		_, err := io.CopyN(io.Discard, r, int64(3*(1<<((flags&7)+1))))
		return err
	}
	if err := skipPalette(header[10]); err != nil {
		return info, err
	}
	skipBlocks := func() error {
		for {
			if err := ctx.Err(); err != nil {
				return err
			}
			n, err := r.ReadByte()
			if err != nil {
				return err
			}
			if n == 0 {
				return nil
			}
			if _, err := io.CopyN(io.Discard, r, int64(n)); err != nil {
				return err
			}
		}
	}
	for {
		if err := ctx.Err(); err != nil {
			return info, err
		}
		tag, err := r.ReadByte()
		if err != nil {
			return info, err
		}
		switch tag {
		case 0x3b:
			if info.Frames == 0 {
				return info, errors.New("GIF has no frames")
			}
			return info, nil
		case 0x21:
			if _, err := r.ReadByte(); err != nil {
				return info, err
			}
			if err := skipBlocks(); err != nil {
				return info, err
			}
		case 0x2c:
			var frame [9]byte
			if _, err := io.ReadFull(r, frame[:]); err != nil {
				return info, err
			}
			x, y := int(binary.LittleEndian.Uint16(frame[0:2])), int(binary.LittleEndian.Uint16(frame[2:4]))
			w, h := int(binary.LittleEndian.Uint16(frame[4:6])), int(binary.LittleEndian.Uint16(frame[6:8]))
			info.Frames++
			info.Pixels += int64(w) * int64(h)
			if w == 0 || h == 0 || x+w > info.Width || y+h > info.Height || info.Frames > ChatGPTWebMaxGIFFrames || info.Pixels > maxPixels {
				return info, errors.New("GIF frames exceed pixel or frame budget")
			}
			if err := skipPalette(frame[8]); err != nil {
				return info, err
			}
			if _, err := r.ReadByte(); err != nil {
				return info, err
			}
			if err := skipBlocks(); err != nil {
				return info, err
			}
		default:
			return info, errors.New("invalid GIF block")
		}
	}
}

// ValidateChatGPTWebAnimationControls checks the public compatibility controls.
// Web generation receives animation instructions, not undocumented HTTP fields.
func ValidateChatGPTWebAnimationControls(format string, frames, duration int) error {
	if format != "gif" && (frames != 0 || duration != 0) {
		return errors.New("frame_count and frame_duration_ms require output_format=gif")
	}
	if frames != 0 && (frames < 2 || frames > ChatGPTWebMaxGIFFrames) {
		return fmt.Errorf("frame_count must be between 2 and %d", ChatGPTWebMaxGIFFrames)
	}
	if duration != 0 && (duration < 20 || duration > 2000) {
		return errors.New("frame_duration_ms must be between 20 and 2000")
	}
	return nil
}

func ChatGPTWebGIFPrompt(prompt string, frames, duration int) string {
	if frames == 0 {
		frames = 9
	}
	if duration == 0 {
		duration = 200
	}
	duration = (duration + 5) / 10 * 10
	return prompt + fmt.Sprintf("\n\nCreate a looping animated GIF with %d frames at %d milliseconds per frame using the image tool's animation capability. Deliver the finished animated GIF. Any requested dimensions apply to each animation frame.", frames, duration)
}

// ProcessChatGPTWebGIF preserves original bytes unless resizing or retiming is
// requested. Resizing composites disposal modes before resizing every frame.
// Callers reserve memory for the inspected frames and complete target canvas.
func ProcessChatGPTWebGIF(ctx context.Context, data []byte, frames, duration, width, height int, approximate bool, output io.Writer) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	animation, err := gif.DecodeAll(bytes.NewReader(data))
	if err != nil {
		return false, err
	}
	if len(animation.Image) < 2 {
		return false, errors.New("upstream did not return an animated GIF")
	}
	if frames > 0 && len(animation.Image) != frames {
		return false, errors.New("GIF frame count does not match the requested frame_count")
	}
	changed := false
	if duration > 0 {
		delay := (duration + 5) / 10
		for i := range animation.Delay {
			if animation.Delay[i] != delay {
				animation.Delay[i] = delay
				changed = true
			}
		}
	}
	if width > 0 && height > 0 && (width != animation.Config.Width || height != animation.Config.Height) {
		bounds := image.Rect(0, 0, animation.Config.Width, animation.Config.Height)
		canvas := image.NewNRGBA(bounds)
		background := chatGPTWebGIFBackground(animation, animation.Image[0])
		draw.Draw(canvas, bounds, &image.Uniform{C: background}, image.Point{}, draw.Src)
		colors := append(color.Palette{color.Transparent}, palette.Plan9[:254]...)
		colors = append(colors, color.White)
		resized := make([]*image.Paletted, len(animation.Image))
		disposal := make([]byte, len(animation.Image))
		for i, frame := range animation.Image {
			if err := ctx.Err(); err != nil {
				return false, err
			}
			mode := byte(0)
			if i < len(animation.Disposal) {
				mode = animation.Disposal[i]
			}
			var previous *image.NRGBA
			if mode == gif.DisposalPrevious {
				previous = image.NewNRGBA(bounds)
				copy(previous.Pix, canvas.Pix)
			}
			draw.Draw(canvas, frame.Bounds(), frame, frame.Bounds().Min, draw.Over)
			crop := bounds
			if int64(bounds.Dx())*int64(height) > int64(bounds.Dy())*int64(width) {
				w := max(1, int(int64(bounds.Dy())*int64(width)/int64(height)))
				crop.Min.X = (bounds.Dx() - w) / 2
				crop.Max.X = crop.Min.X + w
			} else {
				h := max(1, int(int64(bounds.Dx())*int64(height)/int64(width)))
				crop.Min.Y = (bounds.Dy() - h) / 2
				crop.Max.Y = crop.Min.Y + h
			}
			scaled := image.NewNRGBA(image.Rect(0, 0, width, height))
			scaler := xdraw.Scaler(xdraw.CatmullRom)
			if approximate {
				scaler = xdraw.ApproxBiLinear
			}
			scaler.Scale(scaled, scaled.Bounds(), canvas, crop, draw.Src, nil)
			resized[i] = image.NewPaletted(scaled.Bounds(), colors)
			draw.FloydSteinberg.Draw(resized[i], scaled.Bounds(), scaled, image.Point{})
			disposal[i] = gif.DisposalBackground
			switch mode {
			case gif.DisposalBackground:
				draw.Draw(canvas, frame.Bounds(), &image.Uniform{C: chatGPTWebGIFBackground(animation, frame)}, image.Point{}, draw.Src)
			case gif.DisposalPrevious:
				canvas = previous
			}
		}
		animation.Image, animation.Disposal = resized, disposal
		animation.Config = image.Config{ColorModel: colors, Width: width, Height: height}
		animation.BackgroundIndex = 0
		changed = true
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if !changed {
		return false, nil
	}
	return true, gif.EncodeAll(output, animation)
}

func chatGPTWebGIFBackground(animation *gif.GIF, frame *image.Paletted) color.Color {
	// Browsers clear transparent animation frames to transparent, even when the
	// logical screen's global palette stores an opaque background color.
	for _, entry := range frame.Palette {
		if _, _, _, alpha := entry.RGBA(); alpha == 0 {
			return color.Transparent
		}
	}
	if p, ok := animation.Config.ColorModel.(color.Palette); ok && int(animation.BackgroundIndex) < len(p) {
		return p[animation.BackgroundIndex]
	}
	return color.Transparent
}
